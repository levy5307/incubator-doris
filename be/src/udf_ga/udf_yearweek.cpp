
#include "udf_ga/udf_yearweek.h"
#include <iostream>
#include <string>
#include "runtime/datetime_value.h"

namespace doris_udf {

IntVal year_week(FunctionContext *context,
                 const DateTimeVal &ts_val,
                 const IntVal &week_start,
                 const IntVal &day_in_first_week) {
    if (ts_val.is_null || week_start.val < 1 || week_start.val > 7 || day_in_first_week.val < 1
            || day_in_first_week.val > 7) {
        return IntVal::null();
    }
    const doris::DateTimeValue &ts_value = doris::DateTimeValue::from_datetime_val(ts_val);
    if (ts_value.is_valid_date()) {
        return ts_value.year_week(week_start.val, day_in_first_week.val);
    }
    return IntVal::null();
}
}
