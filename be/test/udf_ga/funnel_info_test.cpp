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

using namespace doris_udf;

static const int64_t ts = 1614047602820L;

class FunnelInfoTest : public testing::Test {
public:
    FunnelInfoTest() = default;

    void SetUp() {
        utils = new FunctionUtils();
        ctx = utils->get_fn_ctx();
    }
    void TearDown() { delete utils; }

    FunctionUtils* utils = nullptr;
    FunctionContext* ctx = nullptr;
    StringVal dst_info;
};

void update_funnel_info(FunctionContext* ctx, StringVal& info_agg_val) {
    BigIntVal from_time(ts);
    BigIntVal time_window(300000L);
    funnel_info_update(ctx, from_time, time_window, SmallIntVal(0), BigIntVal(ts), &info_agg_val);
    funnel_info_update(ctx, from_time, time_window, SmallIntVal(1), BigIntVal(ts), &info_agg_val);
    funnel_info_update(ctx, from_time, time_window, SmallIntVal(2), BigIntVal(ts + 10000), &info_agg_val);
    funnel_info_update(ctx, from_time, time_window, SmallIntVal(4), BigIntVal(ts + 20000), &info_agg_val);
    funnel_info_update(ctx, from_time, time_window, SmallIntVal(4), BigIntVal(ts + 1020000), &info_agg_val);
}

void check_empty_data(const StringVal& dst_info) {
    ASSERT_EQ(nullptr, dst_info.ptr);
    ASSERT_FALSE(dst_info.is_null);
    ASSERT_EQ(0, dst_info.len);
}

TEST_F(FunnelInfoTest, parse_data) {
    funnel_info_init(ctx, &dst_info);

    BigIntVal from_time(ts);
    BigIntVal time_window(300000L);

    FunnelInfoAgg info_agg;
    parse(dst_info, info_agg);
    EXPECT_EQ(0, info_agg._start_of_day);
    EXPECT_EQ(0, info_agg._time_window);
    EXPECT_EQ(0, info_agg._events.size());

    funnel_info_update(ctx, from_time, time_window, SmallIntVal(1), BigIntVal(ts), &dst_info);
    parse(dst_info, info_agg);
    EXPECT_EQ(get_start_of_day(ts / 1000), info_agg._start_of_day);
    EXPECT_EQ(300, info_agg._time_window);
    EXPECT_EQ(1, info_agg._events.size());
    std::vector<Event> tmp1(info_agg._events.begin(), info_agg._events.end());
    EXPECT_EQ(0, tmp1[0]._step);
    EXPECT_EQ(ts / 1000, tmp1[0]._ts);

    funnel_info_update(ctx, from_time, time_window, SmallIntVal(2), BigIntVal(ts + 10000), &dst_info);
    parse(dst_info, info_agg);
    EXPECT_EQ(get_start_of_day(ts / 1000), info_agg._start_of_day);
    EXPECT_EQ(300, info_agg._time_window);
    EXPECT_EQ(2, info_agg._events.size());
    std::vector<Event> tmp2(info_agg._events.begin(), info_agg._events.end());
    EXPECT_EQ(0, tmp2[0]._step);
    EXPECT_EQ(ts / 1000, tmp2[0]._ts);
    EXPECT_EQ(1, tmp2[1]._step);
    EXPECT_EQ((ts + 10000) / 1000, tmp2[1]._ts);
}

TEST_F(FunnelInfoTest, funnel_info_init) {
    funnel_info_init(ctx, &dst_info);
    ASSERT_NO_FATAL_FAILURE(check_empty_data(dst_info));
}

TEST_F(FunnelInfoTest, funnel_info_update_invalid) {
    BigIntVal from_time(ts);
    BigIntVal time_window(300000L);

    funnel_info_init(ctx, &dst_info);
    dst_info.len = kMaxFunnelInfoSize + 1;
    funnel_info_update(ctx, from_time, time_window, SmallIntVal(1), BigIntVal(ts), &dst_info);
    ASSERT_EQ(nullptr, dst_info.ptr);
    ASSERT_FALSE(dst_info.is_null);
    ASSERT_EQ(kMaxFunnelInfoSize + 1, dst_info.len);

    funnel_info_init(ctx, &dst_info);
    funnel_info_update(ctx, from_time, time_window, SmallIntVal(0), BigIntVal(ts), &dst_info);
    ASSERT_NO_FATAL_FAILURE(check_empty_data(dst_info));

    funnel_info_init(ctx, &dst_info);
    funnel_info_update(ctx, from_time, time_window, SmallIntVal(kMaxStep + 1), BigIntVal(ts), &dst_info);
    ASSERT_NO_FATAL_FAILURE(check_empty_data(dst_info));

    funnel_info_init(ctx, &dst_info);
    funnel_info_update(ctx, from_time, time_window, SmallIntVal(3), BigIntVal(ts), &dst_info);
    ASSERT_NO_FATAL_FAILURE(check_empty_data(dst_info));

    funnel_info_init(ctx, &dst_info);
    funnel_info_update(ctx, from_time, time_window, SmallIntVal(1), BigIntVal(ts + kMaxFunnelSupportInterval + 1), &dst_info);
    ASSERT_NO_FATAL_FAILURE(check_empty_data(dst_info));
}

TEST_F(FunnelInfoTest, funnel_info_update) {
    BigIntVal from_time(ts);
    BigIntVal time_window(300000L);

    funnel_info_init(ctx, &dst_info);

    funnel_info_update(ctx, from_time, time_window, SmallIntVal(1), BigIntVal(ts), &dst_info);
    EXPECT_EQ(ts, *(int64*)dst_info.ptr);
    EXPECT_EQ(300000L, *(int64*)(dst_info.ptr + 8));
    EXPECT_EQ(0, *(uint8*)(dst_info.ptr + 16));
    EXPECT_EQ(ts, *(int64*)(dst_info.ptr + 17));
    EXPECT_EQ(25, dst_info.len);

    funnel_info_update(ctx, from_time, time_window, SmallIntVal(2), BigIntVal(ts + 10000), &dst_info);
    EXPECT_EQ(1, *(uint8*)(dst_info.ptr + 25));
    EXPECT_EQ(ts + 10000, *(int64*)(dst_info.ptr + 26));
    EXPECT_EQ(34, dst_info.len);

    funnel_info_update(ctx, from_time, time_window, SmallIntVal(4), BigIntVal(ts + 20000), &dst_info);
    EXPECT_EQ(2, *(uint8*)(dst_info.ptr + 34));
    EXPECT_EQ(ts + 20000, *(int64*)(dst_info.ptr + 35));
    EXPECT_EQ(43, dst_info.len);

    funnel_info_update(ctx, from_time, time_window, SmallIntVal(4), BigIntVal(ts + 1020000), &dst_info);
    EXPECT_EQ(2, *(uint8*)(dst_info.ptr + 43));
    EXPECT_EQ(ts + 1020000,*(uint64*)(dst_info.ptr + 44));
    EXPECT_EQ(52, dst_info.len);
}

TEST_F(FunnelInfoTest, funnel_info_merge) {
    funnel_info_init(ctx, &dst_info);

    BigIntVal from_time(ts);
    BigIntVal time_window(300000L);

    {
        StringVal src;
        funnel_info_update(ctx, from_time, time_window, SmallIntVal(1), BigIntVal(ts), &src);
        funnel_info_merge(ctx, src, &dst_info);
    }
    EXPECT_EQ(ts, *(int64*)dst_info.ptr);
    EXPECT_EQ(300000L, *(int64*)(dst_info.ptr + 8));
    EXPECT_EQ(0, *(uint8*)(dst_info.ptr + 16));
    EXPECT_EQ(ts, *(int64*)(dst_info.ptr + 17));
    EXPECT_EQ(25, dst_info.len);

    {
        StringVal src;
        funnel_info_update(ctx, from_time, time_window, SmallIntVal(2), BigIntVal(ts + 10000), &dst_info);
        funnel_info_merge(ctx, src, &dst_info);
    }
    EXPECT_EQ(1, *(uint8*)(dst_info.ptr + 25));
    EXPECT_EQ(ts + 10000, *(int64*)(dst_info.ptr + 26));
    EXPECT_EQ(34, dst_info.len);

    {
        StringVal src;
        funnel_info_update(ctx, from_time, time_window, SmallIntVal(4), BigIntVal(ts + 20000), &dst_info);
        funnel_info_merge(ctx, src, &dst_info);
    }
    EXPECT_EQ(2, *(uint8*)(dst_info.ptr + 34));
    EXPECT_EQ(ts + 20000, *(int64*)(dst_info.ptr + 35));
    EXPECT_EQ(43, dst_info.len);
}

TEST_F(FunnelInfoTest, funnel_info_finalize) {
    funnel_info_init(ctx, &dst_info);
    update_funnel_info(ctx, dst_info);
    StringVal rs = funnel_info_finalize(ctx, dst_info);
    EXPECT_EQ(12, rs.len);
    EXPECT_EQ(2, *(int16_t*)rs.ptr);
    EXPECT_EQ(258, *(int16_t*)(rs.ptr + 2));
    EXPECT_EQ(1, *(int16_t*)(rs.ptr + 4));
    EXPECT_EQ(257, *(int16_t*)(rs.ptr + 6));
    EXPECT_EQ(0, *(int16_t*)(rs.ptr + 8));
    EXPECT_EQ(256, *(int16_t*)(rs.ptr + 10));
}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
