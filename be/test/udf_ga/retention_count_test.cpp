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

#include "udf_ga//retention_info.h"
#include "udf_ga/retention_count.h"
#include "testutil/function_utils.h"
#include <gtest/gtest.h>

namespace doris {

class RetentionCountTest : public testing::Test {
public:
    RetentionCountTest() = default;

    void SetUp() {
        utils = new FunctionUtils();
        ctx = utils->get_fn_ctx();
    }

    void TearDown() { delete utils; }

    FunctionUtils *utils;
    FunctionContext *ctx;
    StringVal dst_count;
};

StringVal generate_retention_info1(FunctionContext* ctx) {
    StringVal dst_info;
    doris_udf::retention_count_init(ctx, &dst_info);
    BigIntVal start_time(1614047612820L);
    StringVal unit("day");
    doris_udf::retention_info_update(ctx, start_time, unit, BigIntVal(1614047612820L), IntVal(1), &dst_info);
    doris_udf::retention_info_update(ctx, start_time, unit, BigIntVal(1614134012000L), IntVal(2), &dst_info);
    doris_udf::retention_info_update(ctx, start_time, unit, BigIntVal(1614220412000L), IntVal(2), &dst_info);
    doris_udf::retention_info_update(ctx, start_time, unit, BigIntVal(1614306812000L), IntVal(1), &dst_info);
    doris_udf::retention_info_update(ctx, start_time, unit, BigIntVal(1614307112000L), IntVal(2), &dst_info);
    return doris_udf::retention_info_finalize(ctx, dst_info);
}

StringVal generate_retention_info2(FunctionContext* ctx) {
    StringVal dst_info;
    doris_udf::retention_count_init(ctx, &dst_info);
    BigIntVal start_time(1614047612820L);
    StringVal unit("day");
    doris_udf::retention_info_update(ctx, start_time, unit, BigIntVal(1614047618320L), IntVal(1), &dst_info);
    doris_udf::retention_info_update(ctx, start_time, unit, BigIntVal(1614134012000L), IntVal(2), &dst_info);
    doris_udf::retention_info_update(ctx, start_time, unit, BigIntVal(1614306856000L), IntVal(1), &dst_info);
    doris_udf::retention_info_update(ctx, start_time, unit, BigIntVal(1614307112000L), IntVal(2), &dst_info);
    return doris_udf::retention_info_finalize(ctx, dst_info);
}

void generate_retention_count(FunctionContext* ctx, StringVal& dst_count) {
    doris_udf::retention_count_init(ctx, &dst_count);
    doris_udf::retention_count_update(ctx, generate_retention_info1(ctx), &dst_count);
    doris_udf::retention_count_update(ctx, generate_retention_info2(ctx), &dst_count);
}

TEST_F(RetentionCountTest, retention_count_init) {
    doris_udf::retention_count_init(ctx, &dst_count);
    EXPECT_FALSE(dst_count.is_null);
    EXPECT_EQ(doris_udf::kRetentionCountBufferSize, dst_count.len);
}

TEST_F(RetentionCountTest, retention_count_update) {
    generate_retention_count(ctx, dst_count);

    uint32_t val1 = *(uint32_t*)(dst_count.ptr + 3 * doris_udf::kRetentionSingleRowSize + 0 * doris_udf::kRetentionCellSize);
    uint32_t val2 = *(uint32_t*)(dst_count.ptr + 0 * doris_udf::kRetentionSingleRowSize + 0 * doris_udf::kRetentionCellSize);
    uint32_t val3 = *(uint32_t*)(dst_count.ptr + 0 * doris_udf::kRetentionSingleRowSize + 2 * doris_udf::kRetentionCellSize);
    uint32_t val4 = *(uint32_t*)(dst_count.ptr + 0 * doris_udf::kRetentionSingleRowSize + 3 * doris_udf::kRetentionCellSize);
    uint32_t val5 = *(uint32_t*)(dst_count.ptr + 0 * doris_udf::kRetentionSingleRowSize + 4 * doris_udf::kRetentionCellSize);
    uint32_t val6 = *(uint32_t*)(dst_count.ptr + 3 * doris_udf::kRetentionSingleRowSize + 0 * doris_udf::kRetentionCellSize);
    uint32_t val7 = *(uint32_t*)(dst_count.ptr + 3 * doris_udf::kRetentionSingleRowSize + 1 * doris_udf::kRetentionCellSize);

    EXPECT_EQ(2, val1);
    EXPECT_EQ(2, val2);
    EXPECT_EQ(2, val3);
    EXPECT_EQ(1, val4);
    EXPECT_EQ(2, val5);
    EXPECT_EQ(2, val6);
    EXPECT_EQ(2, val7);
}

TEST_F(RetentionCountTest, retention_count_merge) {
    StringVal src_count;
    doris_udf::retention_count_init(ctx, &src_count);
    StringVal src_info = generate_retention_info2(ctx);
    doris_udf::retention_count_update(ctx, src_info, &src_count);
    generate_retention_count(ctx, dst_count);
    doris_udf::retention_count_merge(ctx, src_count, &dst_count);

    uint32_t* val1 = (uint32_t*)(dst_count.ptr + 3 * doris_udf::kRetentionSingleRowSize + 0 * doris_udf::kRetentionCellSize);
    uint32_t* val2 = (uint32_t*)(dst_count.ptr + 0 * doris_udf::kRetentionSingleRowSize + 0 * doris_udf::kRetentionCellSize);
    uint32_t* val3 = (uint32_t*)(dst_count.ptr + 0 * doris_udf::kRetentionSingleRowSize + 2 * doris_udf::kRetentionCellSize);
    uint32_t* val4 = (uint32_t*)(dst_count.ptr + 0 * doris_udf::kRetentionSingleRowSize + 3 * doris_udf::kRetentionCellSize);
    uint32_t* val5 = (uint32_t*)(dst_count.ptr + 0 * doris_udf::kRetentionSingleRowSize + 4 * doris_udf::kRetentionCellSize);
    uint32_t* val6 = (uint32_t*)(dst_count.ptr + 3 * doris_udf::kRetentionSingleRowSize + 0 * doris_udf::kRetentionCellSize);
    uint32_t* val7 = (uint32_t*)(dst_count.ptr + 3 * doris_udf::kRetentionSingleRowSize + 1 * doris_udf::kRetentionCellSize);

    EXPECT_EQ(3, *val1);
    EXPECT_EQ(3, *val2);
    EXPECT_EQ(3, *val3);
    EXPECT_EQ(1, *val4);
    EXPECT_EQ(3, *val5);
    EXPECT_EQ(3, *val6);
    EXPECT_EQ(3, *val7);
}

TEST_F(RetentionCountTest, retention_count_finalize) {
    generate_retention_count(ctx, dst_count);
    StringVal rs = doris_udf::retention_count_finalize(ctx, dst_count);
    EXPECT_EQ(StringVal("0:-1:2;0:1:2;0:2:1;0:3:2;3:-1:2;3:0:2;"), rs);
}

TEST_F(RetentionCountTest, parse_retention_row_column) {
    for (int i = 0; i < 1000; ++i) {
        int16_t row, column;
        for (int v = -1; v <= std::numeric_limits<int16_t>::max(); ++v) {
            doris_udf::parse_retention_row_column(v, &row, &column);
        }
    }
}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
