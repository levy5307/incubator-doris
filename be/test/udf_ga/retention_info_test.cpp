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

using namespace doris_udf;

static const int64_t ts = 1614047612;

class RetentionInfoTest : public testing::Test {
public:
    RetentionInfoTest() = default;

    void SetUp() {
        utils = new FunctionUtils();
        ctx = utils->get_fn_ctx();
    }

    void TearDown() { delete utils; }

    void update_retention_info(FunctionContext* ctx, int event_type, const char* unit,
                               const std::vector<int64_t> intervals,
                               const std::vector<int64_t> offsets,
                               StringVal* dst) {
        ASSERT_EQ(intervals.size(), offsets.size());
        BigIntVal start_time(ts * 1000);
        for (int i = 0; i < intervals.size(); ++i) {
            retention_info_update(ctx, start_time, StringVal(unit),
                                  BigIntVal((ts + intervals[i] * 86400 + offsets[i]) * 1000), IntVal(event_type),
                                  dst);
        }
    }

    FunctionUtils* utils = nullptr;
    FunctionContext* ctx = nullptr;
    StringVal dst_info;
};

void check_empty_data(const StringVal& dst_info) {
    ASSERT_FALSE(dst_info.is_null);
    ASSERT_NE(dst_info.ptr, nullptr);
    ASSERT_EQ(kRetentionInfoBufferSize, dst_info.len);
    for (int i = 0; i < kRetentionInfoBufferSize; ++i) {
        ASSERT_EQ(0, *(dst_info.ptr + i));
    }
}

TEST_F(RetentionInfoTest, add_first_event) {
    retention_info_init(ctx, &dst_info);
    for (int16_t index = 0; index < kRetentionEventCount; ++index) {
        add_first_event(index, ts + index, &dst_info);
    }
    for (int16_t index = 0; index < kRetentionEventCount; ++index) {
        ASSERT_EQ(ts + index, *(uint32_t*)(dst_info.ptr + index * kRetentionEventCellSize)) << index;;
        ASSERT_EQ(0, *(uint32_t*)(dst_info.ptr + (index + kRetentionEventCount) * kRetentionEventCellSize));
    }
    ASSERT_FALSE(*(bool*)(dst_info.ptr + kSameEventIndex));
}

TEST_F(RetentionInfoTest, add_second_event) {
    retention_info_init(ctx, &dst_info);
    for (int16_t index = 0; index < kRetentionEventCount; ++index) {
        add_second_event(index, ts - index, &dst_info);
    }
    for (int16_t index = 0; index < kRetentionEventCount; ++index) {
        ASSERT_EQ(0, *(uint32_t*)(dst_info.ptr + index * kRetentionEventCellSize));
        ASSERT_EQ(ts - index, *(uint32_t*)(dst_info.ptr + (index + kRetentionEventCount) * kRetentionEventCellSize));
    }
    ASSERT_FALSE(*(bool*)(dst_info.ptr + kSameEventIndex));
}

TEST_F(RetentionInfoTest, add_events) {
    retention_info_init(ctx, &dst_info);
    set_same_event(&dst_info);
    for (int16_t index = 0; index < kRetentionEventCount; ++index) {
        add_first_event(index, ts + index, &dst_info);
        add_second_event(index, ts - index, &dst_info);
    }
    for (int16_t index = 0; index < kRetentionEventCount; ++index) {
        ASSERT_EQ(ts + index, *(uint32_t*)(dst_info.ptr + index * kRetentionEventCellSize));
        ASSERT_EQ(ts - index, *(uint32_t*)(dst_info.ptr + (index + kRetentionEventCount) * kRetentionEventCellSize));
    }
    ASSERT_TRUE(*(bool*)(dst_info.ptr + kSameEventIndex));
}

TEST_F(RetentionInfoTest, retention_info_init) {
    retention_info_init(ctx, &dst_info);
    check_empty_data(dst_info);
}

TEST_F(RetentionInfoTest, retention_info_update_invalid) {
    retention_info_init(ctx, &dst_info);
    BigIntVal start_time(ts);
    StringVal unit("day");

    retention_info_update(ctx, start_time, unit, BigIntVal(ts * 1000), IntVal(0), &dst_info);
    check_empty_data(dst_info);

    retention_info_update(ctx, start_time, unit, BigIntVal(ts * 1000), IntVal(4), &dst_info);
    check_empty_data(dst_info);

    retention_info_update(ctx, start_time, unit, BigIntVal(ts * 1000 - 86400 * 1000), IntVal(0), &dst_info);
    check_empty_data(dst_info);

    retention_info_update(ctx, start_time, unit, BigIntVal(ts * 1000 + (int64_t)kRetentionEventCount * 86400 * 1000), IntVal(0), &dst_info);
    check_empty_data(dst_info);
}

TEST_F(RetentionInfoTest, retention_info_update_simple) {
    BigIntVal start_time(ts * 1000);
    StringVal unit("day");

    retention_info_init(ctx, &dst_info);
    retention_info_update(ctx, start_time, unit, BigIntVal(ts * 1000), IntVal(1), &dst_info);
    ASSERT_EQ(ts, *(uint32_t*)(dst_info.ptr + 0 * kRetentionEventCellSize));

    retention_info_init(ctx, &dst_info);
    retention_info_update(ctx, start_time, unit, BigIntVal(ts * 1000), IntVal(2), &dst_info);
    ASSERT_EQ(ts, *(uint32_t*)(dst_info.ptr + (0 + kRetentionEventCount) * kRetentionEventCellSize));

    retention_info_init(ctx, &dst_info);
    retention_info_update(ctx, start_time, unit, BigIntVal(ts * 1000 + 1 * 86400 * 1000), IntVal(3), &dst_info);
    ASSERT_EQ(ts + 1 * 86400, *(uint32_t*)(dst_info.ptr + 1 * kRetentionEventCellSize));
    ASSERT_EQ(ts + 1 * 86400, *(uint32_t*)(dst_info.ptr + (1 + kRetentionEventCount) * kRetentionEventCellSize));
    ASSERT_TRUE(*(bool*)(dst_info.ptr + kSameEventIndex));
}

TEST_F(RetentionInfoTest, retention_info_update_more) {
    BigIntVal start_time(ts * 1000);
    StringVal unit("day");

    for (int event_type = 1; event_type <= 3; ++event_type) {
        retention_info_init(ctx, &dst_info);
        // update data
        for (int64_t i = 0; i < kRetentionEventCount; ++i) {
            retention_info_update(ctx, start_time, unit,
                                  BigIntVal(ts * 1000 + i * 86400 * 1000),
                                  IntVal(event_type), &dst_info);
        }

        // check value
        for (int i = 0; i < kRetentionEventCount; ++i) {
            uint32_t first_ts = 0, second_ts = 0;
            bool same_event = false;
            if (event_type == 1) {
                first_ts = ts + i * 86400;
            } else if (event_type == 2) {
                second_ts = ts + i * 86400;
            } else {
                first_ts = ts + i * 86400;
                second_ts = ts + i * 86400;
                same_event = true;
            }
            ASSERT_EQ(first_ts, *(uint32_t*)(dst_info.ptr + i * kRetentionEventCellSize)) << event_type << " " << i << std::endl;
            ASSERT_EQ(second_ts, *(uint32_t*)(dst_info.ptr + (i + kRetentionEventCount) * kRetentionEventCellSize));
            ASSERT_EQ(same_event, *(bool*)(dst_info.ptr + kSameEventIndex));
        }
    }
}

TEST_F(RetentionInfoTest, retention_info_merge_simple) {
    {
        StringVal src_info;
        retention_info_init(ctx, &src_info);
        retention_info_init(ctx, &dst_info);
        update_retention_info(ctx, 1, "day", {1, 2, 3, 4}, {0, 100, 200, 300}, &dst_info);
        update_retention_info(ctx, 1, "day", {2, 3, 4, 5}, {-100, 200, 400, 0}, &src_info);
        retention_info_merge(ctx, src_info, &dst_info);

        ASSERT_EQ(0, *(uint32_t*)(dst_info.ptr + 0 * kRetentionEventCellSize));
        ASSERT_EQ(ts + 1 * 86400, *(uint32_t*)(dst_info.ptr + 1 * kRetentionEventCellSize));
        ASSERT_EQ(ts + 2 * 86400 - 100, *(uint32_t*)(dst_info.ptr + 2 * kRetentionEventCellSize));
        ASSERT_EQ(ts + 3 * 86400 + 200, *(uint32_t*)(dst_info.ptr + 3 * kRetentionEventCellSize));
        ASSERT_EQ(ts + 4 * 86400 + 300, *(uint32_t*)(dst_info.ptr + 4 * kRetentionEventCellSize));
        ASSERT_EQ(ts + 5 * 86400, *(uint32_t*)(dst_info.ptr + 5 * kRetentionEventCellSize));
        ASSERT_EQ(0, *(uint32_t*)(dst_info.ptr + 6 * kRetentionEventCellSize));
    }

    {
        StringVal src_info;
        retention_info_init(ctx, &src_info);
        retention_info_init(ctx, &dst_info);
        update_retention_info(ctx, 2, "day", {1, 2, 3, 4}, {0, 100, 200, 300}, &dst_info);
        update_retention_info(ctx, 2, "day", {2, 3, 4, 5}, {-100, 200, 400, 0}, &src_info);
        retention_info_merge(ctx, src_info, &dst_info);

        ASSERT_EQ(0, *(uint32_t*)(dst_info.ptr + (0 + kRetentionEventCount) * kRetentionEventCellSize));
        ASSERT_EQ(ts + 1 * 86400, *(uint32_t*)(dst_info.ptr + (1 + kRetentionEventCount) * kRetentionEventCellSize));
        ASSERT_EQ(ts + 2 * 86400 + 100, *(uint32_t*)(dst_info.ptr + (2 + kRetentionEventCount) * kRetentionEventCellSize));
        ASSERT_EQ(ts + 3 * 86400 + 200, *(uint32_t*)(dst_info.ptr + (3 + kRetentionEventCount) * kRetentionEventCellSize));
        ASSERT_EQ(ts + 4 * 86400 + 400, *(uint32_t*)(dst_info.ptr + (4 + kRetentionEventCount) * kRetentionEventCellSize));
        ASSERT_EQ(ts + 5 * 86400, *(uint32_t*)(dst_info.ptr + (5 + kRetentionEventCount) * kRetentionEventCellSize));
        ASSERT_EQ(0, *(uint32_t*)(dst_info.ptr + (6 + kRetentionEventCount) * kRetentionEventCellSize));
    }

    {
        retention_info_init(ctx, &dst_info);
        {
            StringVal src_info;
            retention_info_init(ctx, &src_info);
            update_retention_info(ctx, 1, "day", {1, 2, 3, 4}, {0, 100, 200, 300}, &dst_info);
            update_retention_info(ctx, 1, "day", {2, 3, 4, 5}, {-100, 200, 400, 0}, &src_info);
            retention_info_merge(ctx, src_info, &dst_info);
        }
        {
            StringVal src_info;
            retention_info_init(ctx, &src_info);
            update_retention_info(ctx, 2, "day", {1, 2, 3, 4}, {0, 100, 200, 300}, &dst_info);
            update_retention_info(ctx, 2, "day", {2, 3, 4, 5}, {-100, 200, 400, 0}, &src_info);
            retention_info_merge(ctx, src_info, &dst_info);
        }
        ASSERT_EQ(0, *(uint32_t*)(dst_info.ptr + 0 * kRetentionEventCellSize));
        ASSERT_EQ(ts + 1 * 86400 , *(uint32_t*)(dst_info.ptr + 1 * kRetentionEventCellSize));
        ASSERT_EQ(ts + 2 * 86400  - 100, *(uint32_t*)(dst_info.ptr + 2 * kRetentionEventCellSize));
        ASSERT_EQ(ts + 3 * 86400  + 200, *(uint32_t*)(dst_info.ptr + 3 * kRetentionEventCellSize));
        ASSERT_EQ(ts + 4 * 86400  + 300, *(uint32_t*)(dst_info.ptr + 4 * kRetentionEventCellSize));
        ASSERT_EQ(ts + 5 * 86400 , *(uint32_t*)(dst_info.ptr + 5 * kRetentionEventCellSize));
        ASSERT_EQ(0, *(uint32_t*)(dst_info.ptr + 6 * kRetentionEventCellSize));

        ASSERT_EQ(0, *(uint32_t*)(dst_info.ptr + (0 + kRetentionEventCount) * kRetentionEventCellSize));
        ASSERT_EQ(ts + 1 * 86400, *(uint32_t*)(dst_info.ptr + (1 + kRetentionEventCount) * kRetentionEventCellSize));
        ASSERT_EQ(ts + 2 * 86400 + 100, *(uint32_t*)(dst_info.ptr + (2 + kRetentionEventCount) * kRetentionEventCellSize));
        ASSERT_EQ(ts + 3 * 86400 + 200, *(uint32_t*)(dst_info.ptr + (3 + kRetentionEventCount) * kRetentionEventCellSize));
        ASSERT_EQ(ts + 4 * 86400 + 400, *(uint32_t*)(dst_info.ptr + (4 + kRetentionEventCount) * kRetentionEventCellSize));
        ASSERT_EQ(ts + 5 * 86400, *(uint32_t*)(dst_info.ptr + (5 + kRetentionEventCount) * kRetentionEventCellSize));
        ASSERT_EQ(0, *(uint32_t*)(dst_info.ptr + (6 + kRetentionEventCount) * kRetentionEventCellSize));
    }
}

TEST_F(RetentionInfoTest, retention_info_finalize) {
    {
        retention_info_init(ctx, &dst_info);
        update_retention_info(ctx, 1, "day", {0, 2}, {0, 86399}, &dst_info);
        update_retention_info(ctx, 2, "day", {0, 1, 3}, {86399, 86399, 300}, &dst_info);
        StringVal rs = retention_info_finalize(ctx, dst_info);
        EXPECT_EQ(12, rs.len);
        EXPECT_EQ(0, *((int16_t*)(rs.ptr + 0)));
        EXPECT_EQ(2, *((int16_t*)(rs.ptr + 2)));
        EXPECT_EQ(3, *((int16_t*)(rs.ptr + 4)));
        EXPECT_EQ(4, *((int16_t*)(rs.ptr + 6)));
        EXPECT_EQ(768, *((int16_t*)(rs.ptr + 8)));
        EXPECT_EQ(769, *((int16_t*)(rs.ptr + 10)));
    }

    {
        retention_info_init(ctx, &dst_info);
        update_retention_info(ctx, 1, "day", {0, 3}, {5500, 45}, &dst_info);
        update_retention_info(ctx, 2, "day", {0, 3}, {86399, 300}, &dst_info);
        StringVal rs = retention_info_finalize(ctx, dst_info);
        EXPECT_EQ(10, rs.len);
        EXPECT_EQ(0, *((int16_t*)(rs.ptr + 0)));
        EXPECT_EQ(2, *((int16_t*)(rs.ptr + 2)));
        EXPECT_EQ(4, *((int16_t*)(rs.ptr + 4)));
        EXPECT_EQ(768, *((int16_t*)(rs.ptr + 6)));
        EXPECT_EQ(769, *((int16_t*)(rs.ptr + 8)));
    }
}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
