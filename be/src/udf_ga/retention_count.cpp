#include "udf_ga/retention_count.h"

#include <stdio.h>

#include "common/compiler_util.h"

using namespace std;

namespace doris_udf {

void retention_count_init(FunctionContext* context, StringVal* val) {
    *val = StringVal(context, kRetentionCountBufferSize);
    memset(val->ptr, 0, kRetentionCountBufferSize);
}

void parse_from_retention_info(const StringVal* val, set<int16_t>* local) {
    int16_t* index = (int16_t*)(val->ptr);
    int16_t* end_index = (int16_t*)(val->ptr + val->len);
    while (index < end_index) {
        local->insert(*index);
        ++index;
    }
}

void retention_count_update(FunctionContext* context, const StringVal& src_info, StringVal* dst_count) {
    set<int16_t> src_row_column;
    parse_from_retention_info(&src_info, &src_row_column);
    int16_t row = -1;
    int16_t column = -1;
    for (const auto& iter : src_row_column) {
        parse_retention_row_column(iter, &row, &column);
        if (UNLIKELY(row >= kRetentionRowCount || column + 1 >= kRetentionColumnCount)) {
            continue;
        }
        ++(*(uint32_t*)(dst_count->ptr + row * kRetentionSingleRowSize + (column + 1) * kRetentionCellSize));
    }
}

void retention_count_merge(FunctionContext* context, const StringVal& src_count,
                           StringVal* dst_count) {
    for (uint32_t index = 0; index < kRetentionCountBufferSize; index += kRetentionCellSize) {
        uint32_t src = *(uint32_t*)(src_count.ptr + index);
        uint32_t& dst = *(uint32_t*)(dst_count->ptr + index);
        dst += src;
    }
}

void parse_retention_row_column(int16_t v, int16_t* row, int16_t* column) {
    if (UNLIKELY(v < 0)) {
        *row = (-v) >> kRowColShift;
        *column = -(-v & kRowColMask);
    } else {
        *row = v >> kRowColShift;
        *column = v & kRowColMask;
    }
}

StringVal retention_count_finalize(FunctionContext* context, const StringVal& agg_count) {
    static const int kMaxSingleDataLength = strlen("123:123:1234567890;");
    static const int kBatchWriteSize = 256;

    StringVal rst;
    char buffer[kBatchWriteSize] = {0};
    size_t len = 0;
    for (uint8_t i = 0; i < kRetentionRowCount; i++) {
        for (uint8_t j = 0; j < kRetentionColumnCount; j++) {
            int index = i * kRetentionSingleRowSize + j * kRetentionCellSize;
            uint32_t count = *(uint32_t*)(agg_count.ptr + index);
            if (count == 0) {
                continue;
            }
            len += sprintf(buffer + len, "%d:%d:%u;", i, j - 1, count);
            if (len + kMaxSingleDataLength > kBatchWriteSize) {
                // Batch write to avoid too frequently data copy
                rst.append(context, (uint8_t*)buffer, len);
                memset(buffer, 0, len);
                len = 0;
            }
        }
    }
    if (len > 0) {
        rst.append(context, (uint8_t*)buffer, len);
    }
    return rst;
}
}  // namespace doris_udf

