#include "udf_ga/short_encode.h"

#include <ctime>

#include "common/logging.h"

using namespace std;

namespace doris_udf {

void parse_from_info(const StringVal& val, std::set<uint16_t>* local) {
    DCHECK(local);
    uint16_t* index = (uint16_t*)(val.ptr);
    uint16_t* end_index = (uint16_t*)(val.ptr + val.len);
    while (index < end_index) {
        local->insert(*index);
        ++index;
    }
}

void decode_row_col(uint16_t v, uint8_t* row, uint8_t* column) {
    *row = v >> kRowColShift;
    *column = v & kRowColMask;
}

uint16_t encode_row_col(uint8_t row, uint8_t column) {
    return (row << kRowColShift) | column;
}

uint32_t get_start_of_day(uint32_t ts) {
    uint32_t mod = (ts - UTC16) % DAY_TIME_SEC;
    return ts - mod;
}

uint32_t get_start_of_week(uint32_t ts) {
    // 1970-01-01为星期四
    const static uint32_t week_offset = 4 * 86400;
    uint32_t mod = (ts + UTC8 - week_offset) % WEEK_TIME_SEC;
    return ts - mod;
}

int16_t get_delta_periods(uint32_t start_time, uint32_t event_time, const StringVal& unit) {
    if (unit.is_null || unit.len == 0) {
        return -1;
    }

    switch (*(char*)unit.ptr) {
    case 'd':
        return ((int32_t)get_start_of_day(event_time) - (int32_t)get_start_of_day(start_time)) / DAY_TIME_SEC;
    case 'w':
        return ((int32_t)get_start_of_week(event_time) - (int32_t)get_start_of_week(start_time)) / WEEK_TIME_SEC;
    case 'm': {
        time_t s = start_time;
        time_t e = event_time;
        std::tm start_tm = *localtime(&s);
        std::tm event_tm = *localtime(&e);
        return event_tm.tm_mon - start_tm.tm_mon;
    }
    default:
        return -1;
    }
}
}  // namespace doris_udf

