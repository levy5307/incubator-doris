// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "testutil/function_utils.h"
#include "udf_ga/funnel_count.h"
#include <gtest/gtest.h>

namespace doris {

class FunnelCountTest : public testing::Test {
public:
    FunnelCountTest() = default;

    void SetUp() {
        utils = new FunctionUtils();
        ctx = utils->get_fn_ctx();
    }
    void TearDown() { delete utils; }

    FunctionUtils* utils;
    FunctionContext* ctx;
    StringVal dst_count;
};

TEST_F(FunnelCountTest, funnel_count_init) {
    doris_udf::funnel_count_init(ctx, &dst_count);
    EXPECT_FALSE(dst_count.is_null);
    EXPECT_EQ(doris_udf::kFunnelCountBufferSize, dst_count.len);
    for (int i = 0; i < dst_count.len; ++i) {
        uint8_t val = *((uint8_t*) (dst_count.ptr + i));
        EXPECT_EQ(0, val);
    }
}

TEST_F(FunnelCountTest, funnel_count_update) {
    doris_udf::funnel_count_init(ctx, &dst_count);

    StringVal funnel_info1;
    int16_t val1 = 139;
    int16_t val2 = -137;
    funnel_info1.append(ctx, (uint8_t*)(&val1), sizeof(val1));
    funnel_info1.append(ctx, (uint8_t*)(&val2), sizeof(val2));
    doris_udf::funnel_count_update(ctx, funnel_info1, &dst_count);
    uint32_t* dst_val1 = (uint32_t*)(dst_count.ptr + 2 * doris_udf::kFunnelSingleRowSize + 11 * doris_udf::kFunnelCellSize);
    uint32_t* dst_val2 = (uint32_t*)(dst_count.ptr + 0 * doris_udf::kFunnelSingleRowSize + 9 * doris_udf::kFunnelCellSize);
    EXPECT_EQ(1, *dst_val1);
    EXPECT_EQ(1, *dst_val2);

    StringVal funnel_info2;
    int16_t val3 = 139;
    int16_t val4 = -137;
    int16_t val5 = 138;
    funnel_info2.append(ctx, (uint8_t*)(&val3), sizeof(val3));
    funnel_info2.append(ctx, (uint8_t*)(&val4), sizeof(val4));
    funnel_info2.append(ctx, (uint8_t*)(&val5), sizeof(val5));
    doris_udf::funnel_count_update(ctx, funnel_info2, &dst_count);
    uint32_t* dst_val3 = (uint32_t*)(dst_count.ptr + 2 * doris_udf::kFunnelSingleRowSize + 10 * doris_udf::kFunnelCellSize);
    EXPECT_EQ(2, *dst_val1);
    EXPECT_EQ(2, *dst_val2);
    EXPECT_EQ(1, *dst_val3);
}
TEST_F(FunnelCountTest, funnel_count_merge) {
    StringVal src_count;
    doris_udf::funnel_count_init(ctx, &src_count);
    uint32_t* src_val1 = (uint32_t*)(src_count.ptr + 2 * doris_udf::kFunnelSingleRowSize + 11 * doris_udf::kFunnelCellSize);
    uint32_t* src_val2 = (uint32_t*)(src_count.ptr + 0 * doris_udf::kFunnelSingleRowSize + 9 * doris_udf::kFunnelCellSize);
    *src_val1 = 2;
    *src_val2 = 1;
    doris_udf::funnel_count_init(ctx, &dst_count);
    uint32_t* dst_val3 = (uint32_t*)(dst_count.ptr + 2 * doris_udf::kFunnelSingleRowSize + 11 * doris_udf::kFunnelCellSize);
    uint32_t* dst_val4 = (uint32_t*)(dst_count.ptr + 0 * doris_udf::kFunnelSingleRowSize + 9 * doris_udf::kFunnelCellSize);
    uint32_t* dst_val5 = (uint32_t*)(dst_count.ptr + 2 * doris_udf::kFunnelSingleRowSize + 3 * doris_udf::kFunnelCellSize);
    *dst_val3 = 3;
    *dst_val4 = 5;
    *dst_val5 = 1;
    doris_udf::funnel_count_merge(ctx, src_count, &dst_count);
    EXPECT_EQ(5, *dst_val3);
    EXPECT_EQ(6, *dst_val4);
    EXPECT_EQ(1, *dst_val5);
}
TEST_F(FunnelCountTest, funnel_count_finalize) {
    doris_udf::funnel_count_init(ctx, &dst_count);
    uint32_t* dst_val3 = (uint32_t*)(dst_count.ptr + 0 * doris_udf::kFunnelSingleRowSize + 0 * doris_udf::kFunnelCellSize);
    uint32_t* dst_val4 = (uint32_t*)(dst_count.ptr + 0 * doris_udf::kFunnelSingleRowSize + 1 * doris_udf::kFunnelCellSize);
    uint32_t* dst_val5 = (uint32_t*)(dst_count.ptr + 0 * doris_udf::kFunnelSingleRowSize + 2 * doris_udf::kFunnelCellSize);
    *dst_val3 = 5;
    *dst_val4 = 4;
    *dst_val5 = 3;
    StringVal rs = doris_udf::funnel_count_finalize(ctx, dst_count);
    EXPECT_EQ(StringVal("-1:1:5;-1:2:4;-1:3:3;"), rs);
}

TEST_F(FunnelCountTest, parse_row_column) {
    int16_t row = -1;
    int16_t column = -1;
    doris_udf::parse_row_column(11, &row, &column);
    EXPECT_EQ(0, row);
    EXPECT_EQ(11, column);
    doris_udf::parse_row_column(-137, &row, &column);
    EXPECT_EQ(-1, row);
    EXPECT_EQ(9, column);
}

TEST_F(FunnelCountTest, parse_row_column_benchmark) {
    for (int i = 0; i < 1000; ++i) {
        int16_t row, column;
        for (int v = -1; v <= std::numeric_limits<int16_t>::max(); ++v) {
            doris_udf::parse_row_column(v, &row, &column);
        }
    }
}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
