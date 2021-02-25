#include <string>

#include "udf_ga/funnel_info.h"

namespace doris_udf {

void parse(const StringVal& aggInfoVal, FunnelInfoAgg& agg) {
    if (aggInfoVal.len <= 0) return;
    uint8_t* index = aggInfoVal.ptr;
    agg._start_time = *((int64_t*)index);
    index += sizeof(int64_t);

    agg._time_window = *((int64_t*)index);
    index += sizeof(int64_t);

    uint8_t* end_index = aggInfoVal.ptr + aggInfoVal.len;
    while (index < end_index) {
        uint8_t step = *((uint8_t*)index);
        index += sizeof(uint8_t);

        int64_t event_time = *((int64_t*)index);
        index += sizeof(int64_t);

        agg.add_event(step, event_time);
    }
}

short make_value_f(int row, int column) {
    if (row < 0) {
        return (short)((-1) * ((-1 * row) * 100 + column));
    } else {
        return (short)(row * 100 + column);
    }
}

void FunnelInfoAgg::get_trim_events(vector<Event> &rst) {
    if (_events.empty()) {
        return;
    }
    _events.sort();
    list<Event>::iterator first = _events.end();
    list<Event>::iterator second = _events.end();
    for (list<Event>::iterator iter = _events.begin(); iter != _events.end(); iter++) {
        if (first == _events.end()) {
            first = iter;
            rst.push_back(*first);
        } else if (second == _events.end()) {
            if (iter->_step == first->_step && get_start_of_day(first->_ts) == get_start_of_day(iter->_ts)) {
                second = iter;
            } else {
                first = iter;
                rst.push_back(*first);
            }
        } else {
            if (iter->_step == first->_step && get_start_of_day(first->_ts) == get_start_of_day(iter->_ts)) {
                second = iter;
            } else {
                rst.push_back(*second);
                first = iter;
                rst.push_back(*first);
                second = _events.end();
            }
        }
    }
    if (second != _events.end()) {
        rst.push_back(*second);
    }
}

StringVal FunnelInfoAgg::output(FunctionContext* context) {

    // 1: trim events
    vector<Event> rst;
    this->get_trim_events(rst);
    // 2: check events if exceeds max_event_count;
    if (rst.size() > MAX_EVENT_COUNT) {
        return StringVal();
    }

    // 3: calc funnel metric
    unordered_set<short> cache;
    int event_count = rst.size();
    for (int i = 0; i < event_count; i++) {
        Event& first_event = rst[i];
        if (first_event._step != 0) {
            continue;
        }
        int row = (int)((get_start_of_day(first_event._ts) - get_start_of_day(_start_time)) / 86400000);
        cache.insert(make_value_f(-1, 0));
        cache.insert(make_value_f(row, 0));
        int target_num = 1;
        for (int j = i + 1; j < event_count; j++) {
            Event event = rst[j];
            long delta = event._ts - first_event._ts;
            if (event._step == target_num && delta <= _time_window) {
                cache.insert(make_value_f(row, target_num));
                cache.insert(make_value_f(-1, target_num));
                target_num++;
            } else if (delta > _time_window) {
                break;
            }
        }
    }
    // 4: make result
    StringVal rst_str;
    const size_t buffer_size = cache.size() * 2;
    uint8_t buffer[buffer_size];
    int index = 0;
    for (unordered_set<short>::iterator iterator = cache.begin(); iterator != cache.end();
         iterator++, index = index + 2) {
        short* short_ptr = (short*) (buffer + index);
        *(short*)short_ptr = *iterator;
    }
    rst_str.append(context, buffer, buffer_size);
    return rst_str;
}

void funnel_info_init(FunctionContext* context, StringVal* val) {
    val->ptr = NULL;
    val->is_null = false;
    val->len = 0;
}

uint8_t get_step(int16_t step_code) {
    for (uint8_t i = 0; i < STEP_CODE_ARRAY_LENGTH; i++) {
        if (step_code == STEP_CODE_ARRAY[i]) {
            return i;
        }
    }
    // should not happen
    return 0;
}

void funnel_info_update(FunctionContext* context, const BigIntVal& from_time, const BigIntVal& time_window,
                        const SmallIntVal& steps, const BigIntVal& event_time, StringVal* info_agg_val) {
    // we think it is unnormal user info when it exceeds 10M, only support 100 day
    if (steps.val <= 0 || info_agg_val->len > 10485760 || event_time.val - from_time.val > max_funnel_support_interval) {
        return;
    }
    if (steps.val <= MAX_STEP_CODE && steps.val > 0 && ((steps.val & (steps.val - 1)) == 0)) {
        int buffer_size =  sizeof(uint8_t) + sizeof(int64_t);
        if (info_agg_val->len == 0) {
            buffer_size = buffer_size + funnel_info_init_length;
        }
        uint8_t buffer[buffer_size];
        uint8_t* ptr_index = buffer;
        if (info_agg_val->len == 0) {
            *(int64_t*) ptr_index = from_time.val;
            ptr_index = ptr_index + sizeof(int64_t);
            *(int64_t*) ptr_index = time_window.val;
            ptr_index = ptr_index + sizeof(int64_t);
        }

        uint8_t step = get_step(steps.val);
        *(uint8_t*) ptr_index = step;
        ptr_index = ptr_index + sizeof(uint8_t);
        *(int64_t*) ptr_index = event_time.val;
        info_agg_val->append(context, buffer, buffer_size);
    }
}

// map side merge and reduce side merge
void funnel_info_merge(FunctionContext* context, const StringVal& src_agginfo_val,
                       StringVal* dest_agginfo_val) {

    if (dest_agginfo_val->len == 0) {
        dest_agginfo_val->append(context, src_agginfo_val.ptr, src_agginfo_val.len);
    } else if (src_agginfo_val.len > funnel_info_init_length) {
        dest_agginfo_val->append(context, src_agginfo_val.ptr + funnel_info_init_length,
                                 src_agginfo_val.len - funnel_info_init_length);
    }
}

StringVal funnel_info_finalize(FunctionContext* context, const StringVal& aggInfoVal) {
    FunnelInfoAgg finalAgg;
    parse(aggInfoVal, finalAgg);
    return finalAgg.output(context);
}
}  // namespace doris_udf
