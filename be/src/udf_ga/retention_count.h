#pragma once

#include "udf/udf.h"

#include <set>

namespace doris_udf {

static const uint8_t kRowColShift = 7;
static const uint8_t kRowColMask = (1 << kRowColShift) - 1;

static const uint8_t kRetentionCellSize = sizeof(uint32_t);
static const uint8_t kRetentionRowCount = 101;
static const uint8_t kRetentionColumnCount = kRetentionRowCount + 1;  // TODO(yingchun): ?
static const uint16_t kRetentionSingleRowSize = kRetentionColumnCount * kRetentionCellSize;
static const uint32_t kRetentionCountBufferSize = kRetentionRowCount * kRetentionSingleRowSize;

void parse_retention_row_column(int16_t v, int16_t* row, int16_t* column);
void parse_from_retention_info(const StringVal* val, std::set<int16_t>* local);

void retention_count_init(FunctionContext* context, StringVal* val);
void retention_count_update(FunctionContext* context, const StringVal& info, StringVal* count_agg);
void retention_count_merge(FunctionContext* context, const StringVal& src_agg_count,
                           StringVal* dst_agg_count);
StringVal retention_count_finalize(FunctionContext* context, const StringVal& agg_count);
}  // namespace doris_udf
