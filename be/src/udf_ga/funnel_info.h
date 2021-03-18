#pragma once

#include <list>
#include <vector>

#include "udf/udf.h"
#include "udf_ga/funnel_count.h"
#include "udf_ga/short_encode.h"

namespace doris_udf {
static const uint16_t kMaxEventCount = 5000;
static const uint16_t kMaxStep = 1024;
static const uint32_t kMaxFunnelInfoSize = 10 << 20;
static const uint8_t kFunnelInfoHeaderSize = 2 * sizeof(int64_t);
static const uint64_t kMaxFunnelSupportInterval = (uint64_t)kFunnelRowCount * 86400 * 1000;

struct Event {
    uint8_t _step;
    uint32_t _ts;
    explicit Event(uint8_t step, uint32_t ts)
        : _step(step), _ts(ts) {
    }

    bool operator < (const Event& other) {
        if (_ts < other._ts) {
            return true;
        }

        if (_ts == other._ts) {
            return _step < other._step;
        }

        return false;
    }

    bool rough_equal(const Event& other) {
        return _step == other._step && get_start_of_day(_ts) == get_start_of_day(other._ts);
    }
};

struct FunnelInfoAgg {
    uint32_t _start_of_day = 0;  // In seconds
    uint32_t _time_window = 0;   // In seconds
    std::list<Event> _events;

    void add_event(uint8_t step, uint32_t ts) {
        _events.emplace_back(Event(step, ts));
    }

    void get_trim_events(std::vector<Event>& rst);

    StringVal output(FunctionContext* context);
};

uint8_t get_step(int16_t step_code);
void parse(const StringVal& aggInfoVal, FunnelInfoAgg& agg);

void funnel_info_init(FunctionContext* context, StringVal* funnelInfoAggVal);
void funnel_info_update(FunctionContext* context, const BigIntVal& from_time, const BigIntVal& time_window,
                        const SmallIntVal& steps, const BigIntVal& event_time, StringVal* info_agg);
void funnel_info_merge(FunctionContext* context, const StringVal& src_agginfo_val,
                       StringVal* dest_agginfo_val);
StringVal funnel_info_finalize(FunctionContext* context, const StringVal& aggInfoVal);
}  // namespace doris_udf

