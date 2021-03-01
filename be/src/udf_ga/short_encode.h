#pragma once

#include "udf/udf.h"

#include <string>
#include <vector>

namespace doris_udf {
static const uint32_t UTC16 = 16 * 60 * 60;
static const uint32_t UTC8 = 8 * 60 * 60;
static const uint32_t DAY_TIME_SEC = 24 * 60 * 60;
static const uint32_t WEEK_TIME_SEC = 24 * 60 * 60 * 7;

uint32_t get_start_of_day(uint32_t ts);
uint32_t get_start_of_week(uint32_t ts);
int16_t get_delta_periods(uint32_t start_time, uint32_t event_time, const StringVal& unit);
}  // namespace doris_udf

