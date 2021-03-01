#include "udf_ga/retention_info.h"

#include <set>

#include "common/compiler_util.h"
#include "udf_ga/retention_count.h"

namespace doris_udf {

void add_first_event(int16_t index, uint32_t event_time, StringVal* info_agg) {
    uint32_t& ts = *(uint32_t*)(info_agg->ptr + index * kRetentionEventCellSize);
    if (ts == 0 || ts > event_time) {
        ts = event_time;
    }
}

void add_second_event(int16_t index, uint32_t event_time, StringVal* info_agg) {
    uint32_t& ts = *(uint32_t*)(info_agg->ptr + (index + kRetentionEventCount) * kRetentionEventCellSize);
    if (ts == 0 || ts < event_time) {
        ts = event_time;
    }
}

void set_same_event(StringVal* info_agg) {
    *(bool*)(info_agg->ptr + kSameEventIndex) = true;
}

void retention_info_init(FunctionContext* context, StringVal* info_val) {
    *info_val = StringVal(context, kRetentionInfoBufferSize);
    memset(info_val->ptr, 0, kRetentionInfoBufferSize);
}

void retention_info_update(FunctionContext* context, const BigIntVal& start_time, const StringVal& unit,
                           const BigIntVal& event_time, const IntVal& type, StringVal* info_agg) {
    if (0 == type.val) {
        return;
    }

    uint32_t st = start_time.val / 1000;  // uint32_t is enough to hold a timestamp in seconds
    uint32_t et = event_time.val / 1000;
    int16_t index = get_delta_periods(st, et, unit);
    if (index < 0 || index >= kRetentionEventCount) {
        return;
    }

    switch (type.val) {
        // TODO(yingchun): add comment
        case 1:
            add_first_event(index, et, info_agg);
            break;
        case 2:
            add_second_event(index, et, info_agg);
            break;
        case 3:
            add_first_event(index, et, info_agg);
            add_second_event(index, et, info_agg);
            set_same_event(info_agg);
            break;
        default:
            break;
    }
}

void retention_info_merge(FunctionContext* context, const StringVal& src_agg_info,
                          StringVal* dst_agg_info) {
    if (src_agg_info.len == 0) {
        return;
    }

    uint32_t* dst_first = (uint32_t*)(dst_agg_info->ptr);
    uint32_t* src_first = (uint32_t*)(src_agg_info.ptr);
    uint32_t* dst_second = (uint32_t*)(dst_agg_info->ptr + kRetentionEventCount * kRetentionEventCellSize);
    uint32_t* src_second = (uint32_t*)(src_agg_info.ptr + kRetentionEventCount * kRetentionEventCellSize);
    for (int16_t i = 0; i < kRetentionEventCount; i++) {
        // make value smaller
        if (*dst_first == 0 || (*src_first > 0 && *dst_first > *src_first)) {
            *dst_first = *src_first;
        }

        // make value larger
        if (*dst_second == 0 || (*src_second > 0 && *dst_second < *src_second)) {
            *dst_second = *src_second;
        }

        ++dst_first;
        ++src_first;
        ++dst_second;
        ++src_second;
    }

    bool* src_same_event = (bool*)(src_agg_info.ptr + kSameEventIndex);
    if (*src_same_event) {
        set_same_event(dst_agg_info);
    }
}

// column maybe negative value
int16_t make_value(int16_t row, int16_t column) {
    if (UNLIKELY(column < 0)) {
        return -(row << kRowColShift) + column;
    } else {
        return (row << kRowColShift) + column;
    }
}

StringVal retention_info_finalize(FunctionContext* context, const StringVal& agg_info) {
    int16_t row_max_index = kRetentionEventCount - 1;
    for (; row_max_index >= 0; row_max_index--) {
        uint32_t ts = *(uint32_t*)(agg_info.ptr + row_max_index * kRetentionEventCellSize);
        if (ts != 0) {
            break;
        }
    }

    int16_t column_max_index = kRetentionEventCount - 1;
    for (; column_max_index >= 0; column_max_index--) {
        uint32_t ts = *(uint32_t*)(agg_info.ptr + (column_max_index + kRetentionEventCount) * kRetentionEventCellSize);
        if (ts != 0) {
            break;
        }
    }

    std::set<int16_t> local_set;
    bool same_event = *(bool*)(agg_info.ptr + kSameEventIndex);
    for (int16_t row = 0; row <= row_max_index; row++) {
        uint32_t first_ts = *(uint32_t*)(agg_info.ptr + row * kRetentionEventCellSize);
        if (first_ts <= 0) {
            continue;
        }

        local_set.insert(make_value(row, -1));

        if (same_event) {
            local_set.insert(make_value(row, 0));
        }

        for (int16_t column = row; column <= column_max_index; column++) {
            uint32_t second_ts = *(uint32_t*)(agg_info.ptr + (column + kRetentionEventCount) * kRetentionEventCellSize);
            if (second_ts > first_ts) {
                local_set.insert(make_value(row, column - row));
            }
        }
    }

    const size_t buffer_size = local_set.size() * 2;
    StringVal rst(context, buffer_size);
    memset(rst.ptr, 0, buffer_size);

    int16_t* ptr = (int16_t*)(rst.ptr);
    for (const auto& iter : local_set) {
        *ptr = iter;
        ++ptr;
    }

    return rst;
}
}  // namespace doris_udf

