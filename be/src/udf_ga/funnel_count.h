#pragma once

#ifndef DORIS_BE_UDF_UDA_FUNNEL_COUNT
#define DORIS_BE_UDF_UDA_FUNNEL_COUNT

#include <iostream>
#include <list>
#include <time.h>
#include <unordered_map>

#include "common/logging.h"
#include "udf/udf.h"
#include "udf_ga/funnel_info.h"

using namespace std;
namespace doris_udf {

const int funnel_row_size = 101;
const int funnel_column_size = 12;
const int cell_size = sizeof(unsigned int);
const int funnel_single_row_length = funnel_column_size * cell_size;
const int funnel_count_buffer_size = funnel_row_size * funnel_single_row_length;

void funnel_count_init(FunctionContext* context, StringVal* val);
void funnel_count_update(FunctionContext* context, const StringVal& src, StringVal* val);
void funnel_count_merge(FunctionContext* context, const StringVal& src, StringVal* dst);
StringVal funnel_count_finalize(FunctionContext* context, const StringVal& val);
inline void parse_row_column(int v, int* row, int* column) {
    if (v > 0) {
        *column = v % 100;
        *row = v / 100;
    } else {
        *column = (-1 * v) % 100;
        *row = (-1) * ((-1) * v / 100);
    }
}

}

#endif
