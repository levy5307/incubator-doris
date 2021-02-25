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
#include "udf_ga/retention_info.h"
#include <gtest/gtest.h>

namespace doris {

class RetentionInfoTest : public testing::Test {
public:
    RetentionInfoTest() = default;

    void SetUp() {
        utils = new FunctionUtils();
        ctx = utils->get_fn_ctx();
    }

    void TearDown() { delete utils; }

    FunctionUtils *utils;
    FunctionContext *ctx;
    StringVal dst_info;
};

void update_retention_info(FunctionContext* ctx, StringVal& dst_info) {
    BigIntVal start_time(1614047612820L);
    StringVal unit("day");
    doris_udf::retention_info_update(ctx, start_time, unit, BigIntVal(1614047612820L), IntVal(1), &dst_info);
    doris_udf::retention_info_update(ctx, start_time, unit, BigIntVal(1614134012000L), IntVal(2), &dst_info);
    doris_udf::retention_info_update(ctx, start_time, unit, BigIntVal(1614220412000L), IntVal(2), &dst_info);
    doris_udf::retention_info_update(ctx, start_time, unit, BigIntVal(1614306812000L), IntVal(1), &dst_info);
    doris_udf::retention_info_update(ctx, start_time, unit, BigIntVal(1614307112000L), IntVal(2), &dst_info);
}

TEST_F(RetentionInfoTest, retention_info_init) {
    doris_udf::retention_info_init(ctx, &dst_info);
    EXPECT_EQ(809, dst_info.len);
    EXPECT_FALSE(dst_info.is_null);
}

TEST_F(RetentionInfoTest, retention_info_update) {
    doris_udf::retention_info_init(ctx, &dst_info);
    update_retention_info(ctx, dst_info);
    int* time1 = (int*)(dst_info.ptr + 0 * sizeof(int));
    int* time2 = (int*)(dst_info.ptr + (1 + array_size) * sizeof(int));
    int* time3 = (int*)(dst_info.ptr + (2 + array_size) * sizeof(int));
    int* time4 = (int*)(dst_info.ptr + 3 * sizeof(int));
    int* time5 = (int*)(dst_info.ptr + (3 + array_size) * sizeof(int));
    EXPECT_EQ(1614047612L, *time1);
    EXPECT_EQ(1614134012L, *time2);
    EXPECT_EQ(1614220412L, *time3);
    EXPECT_EQ(1614306812L, *time4);
    EXPECT_EQ(1614307112L, *time5);
}

TEST_F(RetentionInfoTest, retention_info_merge) {
    doris_udf::retention_info_init(ctx, &dst_info);
    update_retention_info(ctx, dst_info);
    StringVal src_info;
    doris_udf::retention_info_init(ctx, &src_info);
    BigIntVal start_time(1614047612820L);
    StringVal unit("day");
    doris_udf::retention_info_update(ctx, start_time, unit, BigIntVal(1614393512000L), IntVal(1), &dst_info);
    doris_udf::retention_info_update(ctx, start_time, unit, BigIntVal(1614394510000L), IntVal(2), &dst_info);
    doris_udf::retention_info_merge(ctx, src_info, &dst_info);

    int* time1 = (int*)(dst_info.ptr + 0 * sizeof(int));
    int* time2 = (int*)(dst_info.ptr + (1 + array_size) * sizeof(int));
    int* time3 = (int*)(dst_info.ptr + (2 + array_size) * sizeof(int));
    int* time4 = (int*)(dst_info.ptr + 3 * sizeof(int));
    int* time5 = (int*)(dst_info.ptr + (3 + array_size) * sizeof(int));
    int* time6 = (int*)(dst_info.ptr + 4 * sizeof(int));
    int* time7 = (int*)(dst_info.ptr + (4 + array_size) * sizeof(int));
    EXPECT_EQ(1614047612L, *time1);
    EXPECT_EQ(1614134012L, *time2);
    EXPECT_EQ(1614220412L, *time3);
    EXPECT_EQ(1614306812L, *time4);
    EXPECT_EQ(1614307112L, *time5);
    EXPECT_EQ(1614393512L, *time6);
    EXPECT_EQ(1614394510L, *time7);
}

TEST_F(RetentionInfoTest, retention_info_finalize) {
    doris_udf::retention_info_init(ctx, &dst_info);
    update_retention_info(ctx, dst_info);
    StringVal rs = doris_udf::retention_info_finalize(ctx, dst_info);
    EXPECT_EQ(12, rs.len);
    EXPECT_EQ(-301, *((short *)(rs.ptr + 0)));
    EXPECT_EQ(-1, *((short *)(rs.ptr + 2)));
    EXPECT_EQ(1, *((short *)(rs.ptr + 4)));
    EXPECT_EQ(2, *((short *)(rs.ptr + 6)));
    EXPECT_EQ(3, *((short *)(rs.ptr + 8)));
    EXPECT_EQ(300, *((short *)(rs.ptr + 10)));
}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}