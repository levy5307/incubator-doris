#pragma once

#include "udf/udf.h"

namespace doris_udf {
static const uint8_t kRowColShift = 7;
static const uint8_t kRowColMask = (1 << kRowColShift) - 1;

static const uint8_t kFunnelCellSize = sizeof(uint32_t);
static const uint8_t kFunnelRowCount = 101;
static const uint8_t kFunnelColumnCount = 12;
static const uint8_t kFunnelSingleRowSize = kFunnelColumnCount * kFunnelCellSize;
static const uint16_t kFunnelCountBufferSize = kFunnelRowCount * kFunnelSingleRowSize;

void parse_row_column(int16_t v, int16_t* row, int16_t* column);

void funnel_count_init(FunctionContext* context, StringVal* val);
void funnel_count_update(FunctionContext* context, const StringVal& src, StringVal* val);
void funnel_count_merge(FunctionContext* context, const StringVal& src, StringVal* dst);
StringVal funnel_count_finalize(FunctionContext* context, const StringVal& val);
}  // namespace doris_udf
