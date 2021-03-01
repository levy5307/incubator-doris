#include "udf_ga/funnel_info.h"

#include <math.h>

#include <string>
#include <unordered_set>

#include "common/compiler_util.h"
#include "common/logging.h"
#include "udf_ga/short_encode.h"

using namespace std;

namespace doris_udf {

void parse(const StringVal& aggInfoVal, FunnelInfoAgg& agg) {
    if (aggInfoVal.len <= 0) {
        return;
    }

    uint8_t* index = aggInfoVal.ptr;
    agg._start_of_day = get_start_of_day(*(int64_t*)index / 1000);
    index += sizeof(int64_t);

    agg._time_window = *((int64_t*)index) / 1000;
    index += sizeof(int64_t);

    agg._events.clear();
    uint8_t* end_index = aggInfoVal.ptr + aggInfoVal.len;
    while (index < end_index) {
        uint8_t step = *(uint8_t*)index;
        index += sizeof(uint8_t);

        uint32_t event_time = *(int64_t*)index / 1000;
        index += sizeof(int64_t);

        agg.add_event(step, event_time);
    }
}

// TODO(yingchun): need unit test
void FunnelInfoAgg::get_trim_events(vector<Event>& rst) {
    // Return if empty
    if (_events.empty()) {
        return;
    }

    // Sort events
    _events.sort();

    // Remove serial duplicate events
    list<Event>::iterator first = _events.end();
    list<Event>::iterator second = _events.end();
    for (list<Event>::iterator iter = _events.begin(); iter != _events.end(); iter++) {
        // Init first
        if (first == _events.end()) {
            first = iter;
            rst.push_back(*first);
            continue;
        }

        // When no serial duplicate event found
        if (second == _events.end()) {
            // New serial duplicate event found
            if (iter->rough_equal(*first)) {
                second = iter;
            // Still no serial duplicate event found
            } else {
                first = iter;
                rst.push_back(*first);
            }
            continue;
        }

        // When serial duplicate event found
        // New serial duplicate event found
        if (iter->rough_equal(*first)) {
            second = iter;
        // When a non-serial duplicate event found
        } else {
            rst.push_back(*second);
            first = iter;
            rst.push_back(*first);
            second = _events.end();
        }
    }
    if (second != _events.end()) {
        rst.push_back(*second);
    }
}

StringVal FunnelInfoAgg::output(FunctionContext* context) {
    // 1. trim events
    vector<Event> rst;
    get_trim_events(rst);

    // 2. check events if exceeds kMaxEventCount
    if (rst.size() > kMaxEventCount) {
        return StringVal();
    }

    // 3. calc funnel matrix
    std::unordered_set<int16_t> cache;
    int event_count = rst.size();
    for (int i = 0; i < event_count; i++) {
        const Event& first_event = rst[i];
        if (first_event._step != 0) {
            continue;
        }
        // TODO(yingchun): why always day? how about week, month?
        int16_t row = (get_start_of_day(first_event._ts) - _start_of_day) / DAY_TIME_SEC;
        if (row < 0 || row > kFunnelRowCount) {
            continue;
        }
        cache.insert(encode_row_col(0, 0));
        cache.insert(encode_row_col(row + 1, 0));
        int16_t target_step = 1;
        for (int j = i + 1; j < event_count; j++) {
            const Event& event = rst[j];
            if (event._ts - first_event._ts > _time_window) {
                break;
            }
            if (event._step != target_step) {
                continue;
            }

            cache.insert(encode_row_col(row + 1, target_step));
            cache.insert(encode_row_col(0, target_step));
            ++target_step;
        }
    }

    // 4. make result
    const size_t buffer_size = cache.size() * 2;
    StringVal rst_str(context, buffer_size);
    memset(rst_str.ptr, 0, buffer_size);

    uint16_t* p = (uint16_t*)(rst_str.ptr);
    for (const auto& elem : cache) {
        *p = elem;
        ++p;
    }

    return rst_str;
}

void funnel_info_init(FunctionContext* context, StringVal* val) {
    DCHECK(context);
    DCHECK(val);
    val->ptr = nullptr;
    val->is_null = false;
    val->len = 0;
}

uint8_t get_step(int16_t step_code) {
    return log2(step_code);
}

void funnel_info_update(FunctionContext* context, const BigIntVal& from_time, const BigIntVal& time_window,
                        const SmallIntVal& steps, const BigIntVal& event_time, StringVal* info_agg_val) {
    DCHECK(context);
    DCHECK(info_agg_val);
    // we think it is unnormal user info when it exceeds 10M, only support 100 days
    if (UNLIKELY(info_agg_val->len > kMaxFunnelInfoSize ||
        (steps.val <= 0 || steps.val > kMaxStep) ||
        (steps.val & (steps.val - 1)) != 0 ||
        event_time.val - from_time.val > kMaxFunnelSupportInterval)) {
        return;
    }

    int buffer_size =  sizeof(uint8_t) + sizeof(int64_t);
    if (info_agg_val->len == 0) {
        buffer_size += kFunnelInfoHeaderSize;
    }
    uint8_t buffer[buffer_size] = {0};
    uint8_t* pbuffer = buffer;
    if (info_agg_val->len == 0) {
        int64_t* funnel_info = (int64_t*)pbuffer;
        *funnel_info = from_time.val;
        ++funnel_info;

        *funnel_info = time_window.val;
        ++funnel_info;

        pbuffer += kFunnelInfoHeaderSize;
    }

    *pbuffer = get_step(steps.val);
    ++pbuffer;
    *(int64_t*)pbuffer = event_time.val;

    info_agg_val->append(context, buffer, buffer_size);
}

// map side merge and reduce side merge
void funnel_info_merge(FunctionContext* context, const StringVal& src_agginfo_val,
                       StringVal* dest_agginfo_val) {
    DCHECK(context);
    DCHECK(dest_agginfo_val);
    if (dest_agginfo_val->len == 0) {
        dest_agginfo_val->append(context, src_agginfo_val.ptr, src_agginfo_val.len);
    } else if (src_agginfo_val.len > kFunnelInfoHeaderSize) {
        dest_agginfo_val->append(context, src_agginfo_val.ptr + kFunnelInfoHeaderSize,
                                 src_agginfo_val.len - kFunnelInfoHeaderSize);
    }
}

StringVal funnel_info_finalize(FunctionContext* context, const StringVal& aggInfoVal) {
    DCHECK(context);
    FunnelInfoAgg finalAgg;
    parse(aggInfoVal, finalAgg);
    return finalAgg.output(context);
}
}  // namespace doris_udf

