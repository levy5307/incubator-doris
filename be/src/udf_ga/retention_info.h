#pragma once

#include "udf/udf.h"
#include "udf_ga/short_encode.h"
#include "udf_ga/retention_count.h"

namespace doris_udf {

static const uint8_t kRetentionEventCellSize = sizeof(uint32_t);
static const uint8_t kRetentionEventCount = kRetentionRowCount;  // TODO(yingchun): optimize
static const uint16_t kSameEventIndex = 2 * kRetentionEventCount * kRetentionEventCellSize;
static const uint16_t kRetentionInfoBufferSize = 2 * kRetentionEventCount * kRetentionEventCellSize + sizeof(bool);

void add_first_event(int16_t index, uint32_t event_time, StringVal* info_agg);
void add_second_event(int16_t index, uint32_t event_time, StringVal* info_agg);
void set_same_event(StringVal* info_agg);

void retention_info_init(FunctionContext* context, StringVal* info_val);
void retention_info_update(FunctionContext* context, const BigIntVal& from_time, const StringVal& unit,
                           const BigIntVal& event_time, const IntVal& type, StringVal* info_agg);
void retention_info_merge(FunctionContext* context, const StringVal& src_agg_info,
                          StringVal* dst_agg_info);
StringVal retention_info_finalize(FunctionContext* context, const StringVal& agg_info);
}  // namespace doris_udf

