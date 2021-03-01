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

#include "udf_ga/short_encode.h"

#include "testutil/function_utils.h"

#include <gtest/gtest.h>

namespace doris {

using namespace doris_udf;

class ShortEncodeSuiteTest : public testing::Test {
public:
    void SetUp() {
        utils = new FunctionUtils();
        ctx = utils->get_fn_ctx();
    }
    void TearDown() { delete utils; }

    FunctionUtils* utils = nullptr;
    FunctionContext* ctx = nullptr;
};

TEST(ShortEncodeTest, get_start_of_day) {
    uint32_t start_of_day = 1614614400;  // Tue Mar  2 00:00:00 CST 2021
    ASSERT_EQ(start_of_day - DAY_TIME_SEC, get_start_of_day(start_of_day - 1));
    for (uint32_t i = 0; i < DAY_TIME_SEC; ++i) {
        ASSERT_EQ(start_of_day, get_start_of_day(start_of_day + i));
    }
    ASSERT_EQ(start_of_day + DAY_TIME_SEC, get_start_of_day(start_of_day + DAY_TIME_SEC));
}

TEST(ShortEncodeTest, get_start_of_week) {
    uint32_t start_of_week = 1614528000;  // Mon Mar  1 00:00:00 CST 2021
    ASSERT_EQ(start_of_week - WEEK_TIME_SEC, get_start_of_week(start_of_week - 1));
    for (uint32_t i = 0; i < WEEK_TIME_SEC; ++i) {
        ASSERT_EQ(start_of_week, get_start_of_week(start_of_week + i));
    }
    ASSERT_EQ(start_of_week + WEEK_TIME_SEC, get_start_of_week(start_of_week + WEEK_TIME_SEC));
}

TEST(ShortEncodeTest, get_delta_periods_invalid) {
    uint32_t ts = 1614614400;  // Tue Mar  2 00:00:00 CST 2021
    ASSERT_EQ(-1, get_delta_periods(ts, ts + 1, StringVal()));
    ASSERT_EQ(-1, get_delta_periods(ts, ts + 1, StringVal("DAY")));
    ASSERT_EQ(-1, get_delta_periods(ts, ts + 1, StringVal("WEEK")));
    ASSERT_EQ(-1, get_delta_periods(ts, ts + 1, StringVal("MONTH")));
    ASSERT_EQ(-1, get_delta_periods(ts, ts + 1, StringVal("bad")));
}

TEST(ShortEncodeTest, get_delta_periods_day) {
    StringVal unit("day");
    uint32_t ts = 1614614400;  // Tue Mar  2 00:00:00 CST 2021
    ASSERT_EQ(-1, get_delta_periods(ts, ts - 1, unit));
    ASSERT_EQ(0, get_delta_periods(ts, ts, unit));
    ASSERT_EQ(0, get_delta_periods(ts, ts + 1, unit));
    ASSERT_EQ(0, get_delta_periods(ts, ts + DAY_TIME_SEC - 1, unit));
    ASSERT_EQ(1, get_delta_periods(ts, ts + DAY_TIME_SEC, unit));
    ASSERT_EQ(1, get_delta_periods(ts, ts + DAY_TIME_SEC * 2 - 1, unit));
    ASSERT_EQ(2, get_delta_periods(ts, ts + DAY_TIME_SEC * 2, unit));
    ASSERT_EQ(2, get_delta_periods(ts, ts + DAY_TIME_SEC * 2 + 1, unit));
}

TEST(ShortEncodeTest, get_delta_periods_week) {
    StringVal unit("week");
    uint32_t ts = 1614528000;  // Mon Mar  1 00:00:00 CST 2021
    ASSERT_EQ(-1, get_delta_periods(ts, ts - 1, unit));
    ASSERT_EQ(0, get_delta_periods(ts, ts, unit));
    ASSERT_EQ(0, get_delta_periods(ts, ts + 1, unit));
    ASSERT_EQ(0, get_delta_periods(ts, ts + WEEK_TIME_SEC - 1, unit));
    ASSERT_EQ(1, get_delta_periods(ts, ts + WEEK_TIME_SEC, unit));
    ASSERT_EQ(1, get_delta_periods(ts, ts + WEEK_TIME_SEC * 2 - 1, unit));
    ASSERT_EQ(2, get_delta_periods(ts, ts + WEEK_TIME_SEC * 2, unit));
    ASSERT_EQ(2, get_delta_periods(ts, ts + WEEK_TIME_SEC * 2 + 1, unit));
}

TEST(ShortEncodeTest, get_delta_periods_month) {
    StringVal unit("month");
    static const uint32_t MARCH_TIME_SEC = 31 * DAY_TIME_SEC;
    static const uint32_t APRIL_TIME_SEC = 30 * DAY_TIME_SEC;
    uint32_t ts = 1614528000;  // Mon Mar  1 00:00:00 CST 2021
    ASSERT_EQ(-1, get_delta_periods(ts, ts - 1, unit));
    ASSERT_EQ(0, get_delta_periods(ts, ts, unit));
    ASSERT_EQ(0, get_delta_periods(ts, ts + 1, unit));
    ASSERT_EQ(0, get_delta_periods(ts, ts + MARCH_TIME_SEC - 1, unit));
    ASSERT_EQ(1, get_delta_periods(ts, ts + MARCH_TIME_SEC, unit));
    ASSERT_EQ(1, get_delta_periods(ts, ts + MARCH_TIME_SEC + APRIL_TIME_SEC - 1, unit));
    ASSERT_EQ(2, get_delta_periods(ts, ts + MARCH_TIME_SEC + APRIL_TIME_SEC, unit));
    ASSERT_EQ(2, get_delta_periods(ts, ts + MARCH_TIME_SEC + APRIL_TIME_SEC + 1, unit));
}

TEST(ShortEncodeTest, decode_encode_row_col) {
    uint8_t row = 0;
    uint8_t column = 0;
    for (uint32_t v = 0; v <= std::numeric_limits<uint16_t>::max(); ++v) {
        decode_row_col(v, &row, &column);
        ASSERT_EQ(v, encode_row_col(row, column));
    }
}

TEST_F(ShortEncodeSuiteTest, parse_from_info) {
    StringVal val(ctx, 100);
    memset(val.ptr, 0, 100);
    uint16_t* ptr = (uint16_t*)val.ptr;
    uint16_t i = 0;
    while (ptr < (uint16_t*)(val.ptr + val.len)) {
        *ptr = i;
        ++ptr;
        ++i;
    }
    std::set<uint16_t> local;
    parse_from_info(val, &local);
    ASSERT_EQ(i, local.size());
    for (int j = 0; j < i; ++j) {
        ASSERT_TRUE(local.count(j) == 1);
    }
}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
