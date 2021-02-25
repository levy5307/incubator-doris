#include "udf_ga/retention_count.h"

namespace doris_udf {

void retention_count_init(FunctionContext* context, StringVal* val) {
    val->ptr = NULL;
    val->is_null = false;
    val->len = 0;
    uint8_t buffer[retention_count_buffer_size] = {0};
    val->append(context, buffer, retention_count_buffer_size);
}

void parse_from_retention_info(const StringVal* val, set<short>* local) {
    uint8_t* index = val->ptr;
    uint8_t* end_index = val->ptr + val->len;
    while (index < end_index) {
        short v = *(short *) index;
        index += sizeof(short);
        local->insert(v);
    }
}

inline short make_value_r(int row, int column) {
    short rst = 0;
    if (column < 0) {
        rst = (short)((-1) * (row * 100 + (-1) * column));
    } else {
        rst = (short)(row * 100 + column);
    }
    return rst;
}

void retention_count_update(FunctionContext* context, const StringVal& src_info, StringVal* dst_count) {
    RetentionCountAgg agg_src;
    set<short> src_row_column;
    parse_from_retention_info(&src_info, &src_row_column);
    int row = -1;
    int column = -1;
    for (set<short>::iterator iter = src_row_column.begin(); iter != src_row_column.end(); iter++) {
        parse_retention_row_column(*iter, &row, &column);
        long* val = (long*)(dst_count->ptr + row * single_row_length + (column + 1) * 8);
        *val = *val + 1;
    }

}

void retention_count_merge(FunctionContext* context, const StringVal& src_count,
                           StringVal* dst_count) {
    for (int i = 0; i < retention_row_size; i++) {
        for (int j = 0; j < retention_column_size; j++) {
            int index = i * single_row_length + j * 8;
            long *src = (long*)(src_count.ptr + index);
            long *dst = (long*)(dst_count->ptr + index);
            *dst = *src + *dst;
        }
    }
}

inline void parse_retention_row_column(int v, int* row, int* column) {
    if (v > 0) {
        *column = v % 100;
        *row = v / 100;
    } else {
        *column = (-1) * ((-1 * v) % 100);
        *row = ((-1) * v / 100);
    }
}

void agg_output(FunctionContext* context, RetentionCountAgg* src, StringVal* val) {
    char buffer[128] = "";

    if (val->ptr != NULL) {
        context->free(val->ptr);
        val->ptr = NULL;
        val->len = 0;
    }

    for (unordered_map<short, long>::iterator iter = src->_result_map.begin();
         iter != src->_result_map.end(); iter++) {
        int v = iter->first;
        int row = 0, column = 0;
        parse_retention_row_column(v, &row, &column);
        int count = iter->second;
        sprintf(buffer, "%d:%d:%d;", row, column, count);
        val->append(context, (uint8_t*)buffer, strlen(buffer));
    }
}

StringVal retention_count_finalize(FunctionContext* context, const StringVal& agg_count) {
    char buffer[256] = "";
    StringVal rst;
    for (int i = 0; i < retention_row_size; i++) {
        for (int j = 0; j < retention_column_size; j++) {
            int index = i * single_row_length + j * 8;
            long count = *(long*)(agg_count.ptr + index);
            if (count != 0) {
                sprintf(buffer, "%d:%d:%ld;", i, j - 1, count);
                rst.append(context, (uint8_t*)buffer, strlen(buffer));
            }
        }
    }
    return rst;
}
}
