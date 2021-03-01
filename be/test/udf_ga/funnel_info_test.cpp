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

#include "udf_ga/funnel_info.h"
#include "testutil/function_utils.h"
#include "gutil/integral_types.h"
#include <gtest/gtest.h>

namespace doris {

class FunnelInfoTest : public testing::Test {
public:
    FunnelInfoTest() = default;

    void SetUp() {
        utils = new FunctionUtils();
        ctx = utils->get_fn_ctx();
    }
    void TearDown() { delete utils; }

    FunctionUtils* utils;
    FunctionContext* ctx;
    StringVal dst_info;
};

void update_funnel_info(FunctionContext* ctx, StringVal& info_agg_val) {
    BigIntVal from_time(1614047602820L);
    BigIntVal time_window(300000L);
    doris_udf::funnel_info_update(ctx, from_time, time_window, SmallIntVal(0), BigIntVal(1614047612820L), &info_agg_val);
    doris_udf::funnel_info_update(ctx, from_time, time_window, SmallIntVal(1), BigIntVal(1614047612820L), &info_agg_val);
    doris_udf::funnel_info_update(ctx, from_time, time_window, SmallIntVal(2), BigIntVal(1614047615820L), &info_agg_val);
    doris_udf::funnel_info_update(ctx, from_time, time_window, SmallIntVal(4), BigIntVal(1614047622820L), &info_agg_val);
    doris_udf::funnel_info_update(ctx, from_time, time_window, SmallIntVal(4), BigIntVal(1614048622820L), &info_agg_val);
}

TEST_F(FunnelInfoTest, make_value_f) {
    EXPECT_EQ(130, doris_udf::make_value_f(1, 2));
    EXPECT_EQ(-131, doris_udf::make_value_f(-1, 3));
}

TEST_F(FunnelInfoTest, make_value_f_benckmark) {
    for (int16_t i = 0; i < 1000; ++i) {
        for (int v = -1; v <= std::numeric_limits<int16_t>::max(); ++v) {
            doris_udf::make_value_f(v, i);
        }
    }
}

TEST_F(FunnelInfoTest, funnel_info_init) {
    doris_udf::funnel_info_init(ctx, &dst_info);
    EXPECT_EQ(NULL, dst_info.ptr);
    EXPECT_FALSE(dst_info.is_null);
    EXPECT_EQ(0, dst_info.len);
}

TEST_F(FunnelInfoTest, funnel_info_update) {
    doris_udf::funnel_info_init(ctx, &dst_info);
    update_funnel_info(ctx, dst_info);
    EXPECT_EQ(52, dst_info.len);
    EXPECT_EQ(1614047602820L, *(int64*)dst_info.ptr);
    EXPECT_EQ(300000L, *(int64*)(dst_info.ptr + 8));
    EXPECT_EQ(0, *(uint8*)(dst_info.ptr + 16));
    EXPECT_EQ(1614047612820L, *(int64*)(dst_info.ptr + 17));
    EXPECT_EQ(1, *(uint8*)(dst_info.ptr + 25));
    EXPECT_EQ(1614047615820L, *(int64*)(dst_info.ptr + 26));
    EXPECT_EQ(2, *(uint8*)(dst_info.ptr + 34));
    EXPECT_EQ(1614047622820L, *(int64*)(dst_info.ptr + 35));
    EXPECT_EQ(2, *(uint8*)(dst_info.ptr + 43));
    EXPECT_EQ(1614048622820L,*(uint64*)(dst_info.ptr + 44));
}

TEST_F(FunnelInfoTest, funnel_info_merge) {
    doris_udf::funnel_info_init(ctx, &dst_info);
    BigIntVal from_time(1614047602820L);
    BigIntVal time_window(300000L);
    doris_udf::funnel_info_update(ctx, from_time, time_window, SmallIntVal(1), BigIntVal(1614047612820L), &dst_info);
    StringVal src_info_agg_val;
    update_funnel_info(ctx, src_info_agg_val);
    doris_udf::funnel_info_merge(ctx, src_info_agg_val, &dst_info);
    EXPECT_EQ(61, dst_info.len);
    EXPECT_EQ(1614047602820L, *((int64*) dst_info.ptr));
    EXPECT_EQ(300000L, *((int64*) (dst_info.ptr + 8)));
}

TEST_F(FunnelInfoTest, funnel_info_finalize) {
    doris_udf::funnel_info_init(ctx, &dst_info);
    update_funnel_info(ctx, dst_info);
    StringVal rs = doris_udf::funnel_info_finalize(ctx, dst_info);
    EXPECT_EQ(12, rs.len);
    EXPECT_EQ(-130, *(int16_t*)rs.ptr);
    EXPECT_EQ(2, *(int16_t*)(rs.ptr + 2));
    EXPECT_EQ(-129, *(int16_t*)(rs.ptr + 4));
    EXPECT_EQ(1, *(int16_t*)(rs.ptr + 6));
    EXPECT_EQ(-128, *(int16_t*)(rs.ptr + 8));
    EXPECT_EQ(0, *(int16_t*)(rs.ptr + 10));
}

TEST_F(FunnelInfoTest, parse) {
    doris_udf::FunnelInfoAgg info_agg;
    StringVal str_val;
    doris_udf::parse(str_val, info_agg);
}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
