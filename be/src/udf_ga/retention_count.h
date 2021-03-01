#pragma once

#include "udf/udf.h"
#include "udf_ga/short_encode.h"

namespace doris_udf {
static const uint8_t kRetentionCellSize = sizeof(uint32_t);
static const uint8_t kRetentionRowCount = 101;
static_assert(kRetentionRowCount< kRowColMask);
static const uint8_t kRetentionColumnCount = kRetentionRowCount + 1;  // TODO(yingchun): same event
static_assert(kRetentionColumnCount < kRowColMask);
static const uint16_t kRetentionSingleRowSize = kRetentionColumnCount * kRetentionCellSize;
static const uint32_t kRetentionCountBufferSize = kRetentionRowCount * kRetentionSingleRowSize;

void retention_count_init(FunctionContext* context, StringVal* val);
void retention_count_update(FunctionContext* context, const StringVal& info, StringVal* count_agg);
void retention_count_merge(FunctionContext* context, const StringVal& src_agg_count,
                           StringVal* dst_agg_count);
StringVal retention_count_finalize(FunctionContext* context, const StringVal& agg_count);
}  // namespace doris_udf
