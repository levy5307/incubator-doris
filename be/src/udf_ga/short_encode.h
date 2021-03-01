#pragma once

#include "udf/udf.h"

#include <set>

namespace doris_udf {
static const uint8_t kRowColShift = 8;
static const uint8_t kRowColMask = (1 << kRowColShift) - 1;

static const int32_t UTC16 = 16 * 60 * 60;
static const int32_t UTC8 = 8 * 60 * 60;
static const int32_t DAY_TIME_SEC = 24 * 60 * 60;
static const int32_t WEEK_TIME_SEC = 24 * 60 * 60 * 7;

void parse_from_info(const StringVal& val, std::set<uint16_t>* local);
void decode_row_col(uint16_t v, uint8_t* row, uint8_t* column);
uint16_t encode_row_col(uint8_t row, uint8_t column);

uint32_t get_start_of_day(uint32_t ts);
uint32_t get_start_of_week(uint32_t ts);
int16_t get_delta_periods(uint32_t start_time, uint32_t event_time, const StringVal& unit);
}  // namespace doris_udf

