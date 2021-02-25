#include "udf_ga/funnel_count.h"

namespace doris_udf {

void funnel_count_init(FunctionContext* context, StringVal* val) {
    val->ptr = NULL;
    val->is_null = false;
    val->len = 0;
    uint8_t buffer[funnel_count_buffer_size] = {0};
    val->append(context, buffer, funnel_count_buffer_size);
}

// parse from "funnelm/8=ZQA="" to FunnelCountAgg
void parse_from_info(const StringVal* val, set<short>* local) {
    uint8_t* index = val->ptr;
    uint8_t* end_index = val->ptr + val->len;
    while (index < end_index) {
        short v = *(short *)index;
        index += 2;
        local->insert(v);
    }
}

// src format:funnelm/8=ZQA=
// dst format:1:2:100; 3:4:301;
void funnel_count_update(FunctionContext* context, const StringVal& src, StringVal* dst_count) {
    if (src.len <= 0) {
        return;
    }
    set<short> src_row_column;
    parse_from_info(&src, &src_row_column);
    int row = -1;
    int column = -1;
    for (set<short>::iterator iter = src_row_column.begin(); iter != src_row_column.end(); iter++) {
        parse_row_column(*iter, &row, &column);
        if (row + 1 >= funnel_row_size || column >= funnel_column_size) {
            continue;
        }
        unsigned int* val = (unsigned int*)(dst_count->ptr + (row + 1) * funnel_single_row_length + column * cell_size);
        *val = *val + 1;
    }
}

void funnel_count_merge(FunctionContext* context, const StringVal& src_count, StringVal* dst_count) {
    // FunnelCountAgg agg_src, agg_dst;
    for (int i = 0; i < funnel_row_size; i++) {
        for (int j = 0; j < funnel_column_size; j++) {
            int index = i * funnel_single_row_length + j * cell_size;
            unsigned int *src = (unsigned int*)(src_count.ptr + index);
            unsigned int *dst = (unsigned int*)(dst_count->ptr + index);
            *dst = *src + *dst;
        }
    }
}

StringVal funnel_count_finalize(FunctionContext* context, const StringVal& val) {
    char buffer[256] = "";
    StringVal rst;
    for (int i = 0; i < funnel_row_size; i++) {
        for (int j = 0; j < funnel_column_size; j++) {
            int index = i * funnel_single_row_length + j * cell_size;
            unsigned int count = *(unsigned int*)(val.ptr + index);
            if (count != 0) {
                sprintf(buffer, "%d:%d:%u;", i - 1, j + 1, count);
                rst.append(context, (uint8_t*)buffer, strlen(buffer));
            }
        }
    }
    return rst;
}
}
