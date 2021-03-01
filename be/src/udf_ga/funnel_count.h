#pragma once

#include "udf/udf.h"
#include "udf_ga/short_encode.h"

namespace doris_udf {
static const uint8_t kFunnelCellSize = sizeof(uint32_t);
static const uint8_t kFunnelRowCount = 101;
static_assert(kFunnelRowCount < kRowColMask);
static const uint8_t kFunnelColumnCount = 12;
static_assert(kFunnelColumnCount < kRowColMask);
static const uint8_t kFunnelSingleRowSize = kFunnelColumnCount * kFunnelCellSize;
static const uint16_t kFunnelCountBufferSize = kFunnelRowCount * kFunnelSingleRowSize;

void funnel_count_init(FunctionContext* context, StringVal* val);
void funnel_count_update(FunctionContext* context, const StringVal& src, StringVal* val);
void funnel_count_merge(FunctionContext* context, const StringVal& src, StringVal* dst);
StringVal funnel_count_finalize(FunctionContext* context, const StringVal& val);
}  // namespace doris_udf
