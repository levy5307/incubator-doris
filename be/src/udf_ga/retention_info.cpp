#include "udf_ga/retention_info.h"

namespace doris_udf {

void add_first_event(long start_time, const StringVal& unit, long event_time, StringVal* info_agg) {
    int n = get_delta_periods(start_time, event_time, unit);
    if (n >= 0 && n < array_size) {
        int* ts = (int*)(info_agg->ptr + n * sizeof(int));
        int time_second = event_time / 1000;
        if (*ts == 0 || *ts > time_second) {
            *ts = time_second;
        }
    }
}

void add_second_event(long start_time, const StringVal& unit, long event_time, StringVal* info_agg) {
    int n = get_delta_periods(start_time, event_time, unit);
    if (n >= 0 && n < array_size) {
        int* ts = (int*)(info_agg->ptr + (n + array_size) * sizeof(int));
        int time_second = event_time / 1000;
        if (*ts == 0 || *ts < time_second) {
            *ts = time_second;
        }
    }
}

void set_same_event(StringVal* info_agg) {
    bool* same_event = (bool *)(info_agg->ptr + 2 * array_size * sizeof(int));
    *same_event = true;
}

void retention_info_init(FunctionContext* context, StringVal* info_val) {
    info_val->ptr = NULL;
    info_val->is_null = false;
    info_val->len = 0;
    init_stringval(context, info_val);
}

void init_stringval(FunctionContext* context, StringVal* info_agg) {
    if (info_agg->len == 0) {
        uint8_t buf[init_buffer_size] = {0};
        info_agg->append(context, (const uint8_t*)buf, init_buffer_size);
    }
}

void retention_info_update(FunctionContext* context, const BigIntVal& start_time, const StringVal& unit,
                           const BigIntVal& event_time, const IntVal& type, StringVal* info_agg) {
    if (0 == type.val) return;
    switch (type.val) {
        case 1:
            add_first_event(start_time.val, unit, event_time.val, info_agg);
            break;
        case 2:
            add_second_event(start_time.val, unit, event_time.val, info_agg);
            break;
        case 3:
            add_first_event(start_time.val, unit, event_time.val, info_agg);
            add_second_event(start_time.val, unit, event_time.val, info_agg);
            set_same_event(info_agg);
            break;
    }
}

void retention_info_merge(FunctionContext* context, const StringVal& src_agg_info,
                          StringVal* dst_agg_info) {
    if (src_agg_info.len == 0) return;
    for (int i = 0; i < array_size; i++) {
        auto first_index = i * sizeof(int);
        int* dst_first = (int*)(dst_agg_info->ptr + first_index);
        int* src_first = (int*)(src_agg_info.ptr + first_index);
        // make value smaller
        if (*dst_first == 0 || (*src_first > 0 && *dst_first > *src_first)) {
            *dst_first = *src_first;
        }

        auto second_index = (i + array_size) * sizeof(int);
        int* dst_second = (int*)(dst_agg_info->ptr + second_index);
        int* src_second = (int*)(src_agg_info.ptr + second_index);
        // make value bigger
        if (*dst_second == 0 || (*src_second > 0 && *dst_second < *src_second)) {
            *dst_second = *src_second;
        }
    }

    int same_event_index = array_size * sizeof(int) * 2;
    bool* src_same_event = (bool*)(src_agg_info.ptr + same_event_index);
    bool* dst_same_event = (bool*)(dst_agg_info->ptr + same_event_index);
    if (*src_same_event) {
        *dst_same_event = true;
    }
}

// column maybe negative value
short make_value(int row, int column) {
    short rst = 0;
    if (column < 0) {
        rst = (short)((-1) * (row * 100 + (-1) * column));
    } else {
        rst = (short)(row * 100 + column);
    }
    // printf("make_value, row:%d, column:%d, rst:%d\n", row, column, rst);
    return rst;
}

StringVal retention_info_finalize(FunctionContext* context, const StringVal& agg_info) {
    int same_event_index = array_size * sizeof(int) * 2;
    bool* same_event = (bool*)(agg_info.ptr + same_event_index);

    // warning!!!
    set<short> local_set;
    int row_max_index = array_size - 1, column_max_index = array_size - 1;
    for (; row_max_index >= 0; row_max_index--) {
        int* ts = (int*)(agg_info.ptr + row_max_index * sizeof(int));
        if (*ts == 0) {
            continue;
        } else {
            break;
        }
    }

    for (; column_max_index >= 0; column_max_index--) {
        int* ts = (int*)(agg_info.ptr + (column_max_index + array_size) * sizeof(int));
        if (*ts == 0) {
            continue;
        } else {
            break;
        }
    }

    for (int row = 0; row <= row_max_index; row++) {
        int* min_ts = (int*)(agg_info.ptr + row * sizeof(int));
        if (*min_ts <= 0) {
            // printf("retention_info_finalize, row:%d\n", row);
            continue;
        }
        local_set.insert(make_value(row, -1));

        if (*same_event) {
            local_set.insert(make_value(row, 0));
        }

        for (int column = row; column <= column_max_index; column++) {
            int* second_ts =
                (int*)(agg_info.ptr + (column + array_size) * sizeof(int));
            if (*second_ts > *min_ts) {
                local_set.insert(make_value(row, column - row));
            }
        }
    }

    StringVal rst;
    const size_t buffer_size = local_set.size() * 2;
    uint8_t buffer[buffer_size];
    int index = 0;
    for (set<short>::iterator iter = local_set.begin(); iter != local_set.end(); iter++, index = index + 2) {
        short* short_ptr = (short*) (buffer + index);
        *(short*)short_ptr = *iter;
    }
    rst.append(context, buffer, buffer_size);
    return rst;
}
}
