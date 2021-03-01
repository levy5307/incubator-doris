#include "udf_ga/short_encode.h"

#include <ctime>

using namespace std;

namespace doris_udf {

vector<string> split(const string& str, const string& delim) {
    vector<string> res;
    if ("" == str) return res;
    char* strs = new char[str.length() + 1];
    strcpy(strs, str.c_str());

    char* d = new char[delim.length() + 1];
    strcpy(d, delim.c_str());

    char* p = strtok(strs, d);
    while (p) {
        string s = p;
        res.push_back(s);
        p = strtok(NULL, d);
    }

    return res;
}

string encode(const unsigned char* str, int bytes) {
    std::string _encode_result;
    const unsigned char* current;
    current = str;
    int origin_bytes = 2 * bytes;
    while (bytes > 2) {
        _encode_result += _base64_table[current[0] >> 2];
        _encode_result += _base64_table[((current[0] & 0x03) << 4) + (current[1] >> 4)];
        _encode_result += _base64_table[((current[1] & 0x0f) << 2) + (current[2] >> 6)];
        _encode_result += _base64_table[current[2] & 0x3f];

        current += 3;
        bytes -= 3;
    }
    if (bytes > 0) {
        _encode_result += _base64_table[current[0] >> 2];
        if (bytes % 3 == 1) {
            _encode_result += _base64_table[(current[0] & 0x03) << 4];
            _encode_result += "==";
        } else if (bytes % 3 == 2) {
            _encode_result += _base64_table[((current[0] & 0x03) << 4) + (current[1] >> 4)];
            _encode_result += _base64_table[(current[1] & 0x0f) << 2];
            _encode_result += "=";
        }
    }

    if (_encode_result.length() < origin_bytes) {
        int cur = _encode_result.length();
        while (cur < origin_bytes) {
            _encode_result += "=";
            cur++;
        }
    }

    return _encode_result;
}

long decode(const char* data, size_t length) {
    const char* current = data;
    char decoded_data[17] = "";
    int ch = 0, i = 0, j = 0, k = 0;
    int origin_len = length;
    while ((ch = *current++) != '\0' && length-- > 0) {
        if (ch == base64_pad) {
            if (*current != '=' && (i % 4) == 1) {
                return -1;
            }
            continue;
        }

        ch = decode_table[ch];
        if (ch == -1) {
            continue;
        } else if (ch == -2) {
            return -1;
        }

        switch (i % 4) {
            case 0:
                decoded_data[j] = ch << 2;
                break;
            case 1:
                decoded_data[j++] |= ch >> 4;
                decoded_data[j] = (ch & 0x0f) << 4;
                break;
            case 2:
                decoded_data[j++] |= ch >> 2;
                decoded_data[j] = (ch & 0x03) << 6;
                break;
            case 3:
                decoded_data[j++] |= ch;
                break;
            default:
                break;
        }

        i++;
    }

    k = j;
    /* mop things up if we ended on a boundary */
    if (ch == base64_pad) {
        switch (i % 4) {
            case 1:
                return 0;
            case 2:
                k++;
            case 3:
                decoded_data[k] = 0;
            default:
                break;
        }
    }

    decoded_data[j] = '\0';
    if (origin_len == 4) {
        return *((short*)decoded_data);
    } else if (origin_len == 8) {
        return *((int*)decoded_data);
    } else if (origin_len == 16) {
        return *((long*)decoded_data);
    } else {
        return -1;
    }
}

uint32_t get_start_of_day(uint32_t ts) {
    uint32_t mod = (ts - UTC16) % DAY_TIME_SEC;
    return ts - mod;
}

uint32_t get_start_of_week(uint32_t ts) {
    // 1900-01-01为星期四
    const static uint32_t week_offset = 4 * 86400;
    uint32_t mod = (ts + UTC8 - week_offset) % WEEK_TIME_SEC;
    return ts - mod;
}

int16_t get_delta_periods(uint32_t start_time, uint32_t event_time, const StringVal& unit) {
    if (unit.is_null || unit.len == 0) {
        return -1;
    }

    switch (*(char*)unit.ptr) {
    case 'd':
        return (int16_t)((get_start_of_day(event_time) - get_start_of_day(start_time)) / DAY_TIME_SEC);
    case 'w':
        return (int16_t)((get_start_of_week(event_time) - get_start_of_week(start_time)) / WEEK_TIME_SEC);
    case 'm': {
        int64_t s = start_time * 1000;
        int64_t e = event_time * 1000;
        std::tm start_tm = *localtime(&s);
        std::tm event_tm = *localtime(&e);
        return (int16_t)(event_tm.tm_mon - start_tm.tm_mon);
    }
    default:
        return -1;
    }
}
}  // namespace doris_udf

