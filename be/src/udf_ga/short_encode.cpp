#include "udf_ga/short_encode.h"

#include <ctime>

using namespace std;

namespace doris_udf {

uint32_t get_start_of_day(uint32_t ts) {
    uint32_t mod = (ts - UTC16) % DAY_TIME_SEC;
    return ts - mod;
}

uint32_t get_start_of_week(uint32_t ts) {
    // 1900-01-01为星期四
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
        return (int16_t)((get_start_of_day(event_time) - get_start_of_day(start_time)) / DAY_TIME_SEC);
    case 'w':
        return (int16_t)((get_start_of_week(event_time) - get_start_of_week(start_time)) / WEEK_TIME_SEC);
    case 'm': {
        int64_t s = start_time * 1000;
        int64_t e = event_time * 1000;
        std::tm start_tm = *localtime(&s);
        std::tm event_tm = *localtime(&e);
        return (int16_t)(event_tm.tm_mon - start_tm.tm_mon);
    }
    default:
        return -1;
    }
}
}  // namespace doris_udf

