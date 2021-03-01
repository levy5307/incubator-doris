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

using namespace doris_udf;

class RetentionCountTest : public testing::Test {
public:
    RetentionCountTest() = default;

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

    StringVal generate_retention_info1(FunctionContext* ctx) {
        StringVal dst_info;
        retention_count_init(ctx, &dst_info);
        update_retention_info(ctx, 1, "day", {0, 2}, {0, 86399}, &dst_info);
        update_retention_info(ctx, 2, "day", {0, 1, 3}, {86399, 86399, 300}, &dst_info);
        return retention_info_finalize(ctx, dst_info);
    }

    StringVal generate_retention_info2(FunctionContext* ctx) {
        StringVal dst_info;
        retention_count_init(ctx, &dst_info);
        update_retention_info(ctx, 1, "day", {0, 3}, {5500, 45}, &dst_info);
        update_retention_info(ctx, 2, "day", {0, 3}, {86399, 300}, &dst_info);
        return retention_info_finalize(ctx, dst_info);
    }

    void generate_retention_count(FunctionContext* ctx, StringVal& dst_count) {
        retention_count_init(ctx, &dst_count);
        retention_count_update(ctx, generate_retention_info1(ctx), &dst_count);
        retention_count_update(ctx, generate_retention_info2(ctx), &dst_count);
    }

    static const int64_t ts = 1614047612;
    FunctionUtils* utils = nullptr;
    FunctionContext* ctx = nullptr;
    StringVal dst_count;
};

TEST_F(RetentionCountTest, retention_count_init) {
    retention_count_init(ctx, &dst_count);
    EXPECT_FALSE(dst_count.is_null);
    EXPECT_EQ(kRetentionCountBufferSize, dst_count.len);
    for (int i = 0; i < kRetentionCountBufferSize; ++i) {
        ASSERT_EQ(0, *(dst_count.ptr + i));
    }
}

TEST_F(RetentionCountTest, retention_count_update) {
    generate_retention_count(ctx, dst_count);

    EXPECT_EQ(2, *(uint32_t*)(dst_count.ptr + 0 * kRetentionSingleRowSize + 0 * kRetentionCellSize));
    EXPECT_EQ(2, *(uint32_t*)(dst_count.ptr + 0 * kRetentionSingleRowSize + 2 * kRetentionCellSize));
    EXPECT_EQ(1, *(uint32_t*)(dst_count.ptr + 0 * kRetentionSingleRowSize + 3 * kRetentionCellSize));
    EXPECT_EQ(2, *(uint32_t*)(dst_count.ptr + 0 * kRetentionSingleRowSize + 4 * kRetentionCellSize));
    EXPECT_EQ(2, *(uint32_t*)(dst_count.ptr + 3 * kRetentionSingleRowSize + 0 * kRetentionCellSize));
    EXPECT_EQ(2, *(uint32_t*)(dst_count.ptr + 3 * kRetentionSingleRowSize + 1 * kRetentionCellSize));
}

TEST_F(RetentionCountTest, retention_count_merge) {
    StringVal src_count;
    retention_count_init(ctx, &src_count);
    StringVal src_info = generate_retention_info2(ctx);
    retention_count_update(ctx, src_info, &src_count);
    generate_retention_count(ctx, dst_count);

    retention_count_merge(ctx, src_count, &dst_count);

    EXPECT_EQ(3, *(uint32_t*)(dst_count.ptr + 3 * kRetentionSingleRowSize + 0 * kRetentionCellSize));
    EXPECT_EQ(3, *(uint32_t*)(dst_count.ptr + 0 * kRetentionSingleRowSize + 0 * kRetentionCellSize));
    EXPECT_EQ(3, *(uint32_t*)(dst_count.ptr + 0 * kRetentionSingleRowSize + 2 * kRetentionCellSize));
    EXPECT_EQ(1, *(uint32_t*)(dst_count.ptr + 0 * kRetentionSingleRowSize + 3 * kRetentionCellSize));
    EXPECT_EQ(3, *(uint32_t*)(dst_count.ptr + 0 * kRetentionSingleRowSize + 4 * kRetentionCellSize));
    EXPECT_EQ(3, *(uint32_t*)(dst_count.ptr + 3 * kRetentionSingleRowSize + 0 * kRetentionCellSize));
}

TEST_F(RetentionCountTest, retention_count_finalize) {
    generate_retention_count(ctx, dst_count);
    StringVal rs = retention_count_finalize(ctx, dst_count);
    EXPECT_EQ(StringVal("0:-1:2;0:1:2;0:2:1;0:3:2;3:-1:2;3:0:2;"), rs);
}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
