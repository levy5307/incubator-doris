#include "udf_ga/funnel_count.h"

#include <stdio.h>

#include <set>

#include "common/compiler_util.h"
#include "common/logging.h"

namespace doris_udf {

void funnel_count_init(FunctionContext* context, StringVal* val) {
    DCHECK(context);
    DCHECK(val);
    *val = StringVal(context, kFunnelCountBufferSize);
    memset(val->ptr, 0, kFunnelCountBufferSize);
}

// src format:funnelm/8=ZQA=
// dst format:1:2:100; 3:4:301;
void funnel_count_update(FunctionContext* context, const StringVal& src, StringVal* dst_count) {
    DCHECK(context);
    DCHECK(dst_count);
    if (src.len <= 0) {
        return;
    }
    std::set<uint16_t> src_row_column;
    parse_from_info(src, &src_row_column);
    uint8_t row = 0;
    uint8_t column = 0;
    for (const auto& iter : src_row_column) {
        decode_row_col(iter, &row, &column);
        if (UNLIKELY(row >= kFunnelRowCount || column >= kFunnelColumnCount)) {
            continue;
        }
        ++(*(uint32_t*)(dst_count->ptr + row * kFunnelSingleRowSize + column * kFunnelCellSize));
    }
}

void funnel_count_merge(FunctionContext* context, const StringVal& src_count, StringVal* dst_count) {
    DCHECK(context);
    DCHECK(dst_count);
    // TODO(yingchun): SIMD
    for (int16_t index = 0; index < kFunnelCountBufferSize; index += kFunnelCellSize) {
        uint32_t src = *(uint32_t*)(src_count.ptr + index);
        uint32_t& dst = *(uint32_t*)(dst_count->ptr + index);
        dst += src;
    }
}

StringVal funnel_count_finalize(FunctionContext* context, const StringVal& val) {
    DCHECK(context);
    static const int kMaxSingleDataLength = strlen("123:123:1234567890;");
    static const int kBatchWriteSize = 256;

    StringVal rst;
    char buffer[kBatchWriteSize] = {0};
    size_t len = 0;
    for (uint8_t i = 0; i < kFunnelRowCount; ++i) {
        for (uint8_t j = 0; j < kFunnelColumnCount; ++j) {
            int index = i * kFunnelSingleRowSize + j * kFunnelCellSize;
            uint32_t count = *(uint32_t*)(val.ptr + index);
            if (count == 0) {
                continue;
            }
            len += sprintf(buffer + len, "%d:%d:%u;", i - 1, j + 1, count);
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
} // namespace doris_udf

