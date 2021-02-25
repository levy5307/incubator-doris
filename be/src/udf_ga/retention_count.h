#pragma once

#ifndef DORIS_BE_UDF_UDA_RETENTION_COUNT
#define DORIS_BE_UDF_UDA_RETENTION_COUNT

#include "udf/udf.h"
#include "udf_ga/short_encode.h"
#include <iostream>
#include <list>
#include <set>
#include <string.h>
#include <unordered_map>
#include <vector>

#include "common/logging.h"

using namespace std;

namespace doris_udf {

const int retention_row_size = 101;
const int retention_column_size = 102;
const int single_row_length = 102 * 8;
const int retention_count_buffer_size = retention_row_size * single_row_length;

struct RetentionCountAgg {
    unordered_map<short, long> _result_map;
};

inline void parse_row_column(int v, int* row, int* column);
void retention_count_init(FunctionContext* context, StringVal* val);
void retention_count_update(FunctionContext* context, const StringVal& info, StringVal* count_agg);
void retention_count_merge(FunctionContext* context, const StringVal& src_agg_count,
                           StringVal* dst_agg_count);
inline void parse_retention_row_column(int v, int* row, int* column);
StringVal retention_count_finalize(FunctionContext* context, const StringVal& agg_count);
short make_value_r(int row, int column);
}

#endif
