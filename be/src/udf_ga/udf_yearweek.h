
#pragma once

#include "udf/udf.h"

namespace doris_udf {

IntVal year_week(FunctionContext *context,
                                   const DateTimeVal &ts_val,
                                   const IntVal &week_start,
                                   const IntVal &day_in_first_week);
}
