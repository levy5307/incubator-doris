#pragma once

#ifndef DORIS_BE_UDF_UDA_FUNNEL_INFO
#define DORIS_BE_UDF_UDA_FUNNEL_INFO

#include "udf/udf.h"
#include <iostream>
#include <list>
#include <set>
#include <string.h>
#include <unordered_set>
#include <vector>

#include "udf_ga/short_encode.h"

using namespace std;
extern int event_distinct_id;
namespace doris_udf {

#define MAX_EVENT_COUNT 5000
#define MAX_STEP_CODE 1024
const uint8_t STEP_CODE_ARRAY_LENGTH = 11;
const int16_t STEP_CODE_ARRAY[STEP_CODE_ARRAY_LENGTH] = {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024};
const int funnel_info_init_length = 2 * sizeof(int64_t);
const int64_t max_funnel_support_interval = 8726400000L;
int64_t get_start_of_day(int64_t ts);

struct Event {
    uint8_t _step;
    int64_t _ts;
    Event(uint8_t step, int64_t ts)
        : _step(step), _ts(ts) {

    }
    bool operator < (Event& other) {
        if (_ts < other._ts) {
            return true;
        } else if (_ts == other._ts) {
            if (_step >= other._step) {
                return false;
            } else if (_step < other._step) {
                return true;
            }
        } else {
            return false;
        }
        return false;
    }
};

struct FunnelInfoAgg {
    list<Event> _events;
    int64_t _start_time;
    int64_t _time_window;

    FunnelInfoAgg(): _start_time(0L), _time_window(0L) {}

    void add_event(Event* e) {
        _events.push_back(*e);
    }

    void add_event(uint8_t step, int64_t ts) {
        _events.emplace_back(step, ts);
    }

    list<Event> get_events() { return _events; }

    void get_trim_events(vector<Event>& rst);

    StringVal output(FunctionContext* context);
};

short make_value_f(int row, int column);

void funnel_info_init(FunctionContext* context, StringVal* funnelInfoAggVal);

void funnel_info_update(FunctionContext* context, const BigIntVal& from_time, const BigIntVal& time_window,
                        const SmallIntVal& steps, const BigIntVal& event_time, StringVal* info_agg);

void funnel_info_merge(FunctionContext* context, const StringVal& src_agginfo_val,
                       StringVal* dest_agginfo_val);

StringVal funnel_info_finalize(FunctionContext* context, const StringVal& aggInfoVal);

FunnelInfoAgg parse(FunctionContext* context, const StringVal& aggInfoVal);

void parse(const StringVal& aggInfoVal, FunnelInfoAgg& agg);

}

#endif
