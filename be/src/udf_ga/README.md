## 路径分析UDAF
1. 函数参数--事件名称, 时间戳, 切分session时长, 起始/终止事件, 决定起始/终止, 最大层数
            exmaple: ["xiangkan", 157000, 60000, load, 0, 10]
            
2. 函数返回值--A&B&C;2-A&B&C&D&A;1-A&D;1 (路径A-->B-->C两次, A-->B-->C-->D-->A一次, A-->D一次)

3. 代码执行步骤:
    
    3.1 根据时间戳对用户行为进行排序
    
    3.2 根据传入的切分session时长参数切分session
    
    3.3 根据决定起始/终止标志位决定起始/终止事件
    
    3.4 遍历用户事件,寻找包含起始/终止事件的session,并根据最大层数进行截断
    
    3.5 统计用户路径并返回
    
4. Example:
    
    4.1 用户的访问行为为[ABCBCDABCABCCBDAABC]
    
    4.2 根据切分时长切分session,结果为[ABC | BC | DABCABC | CBDA | ABC]
    
    4.3 寻找以A事件为起点, 最大层数为10的session路径[ABC | ABCABC | A | ABC]
    
    4.4 统计结果返回A&B&C;2-A&B&C&A&B&C;1-A;1

## 留存分析UDAF
1. 函数参数说明:
 -start_time: 留存的开始时间 
 - uinit:留存的时间单位，天，周，月，目前能支持的数据量在100天
 - event_time:选取的事件所发生的时间
 - type:类型，如果是1，表示首次发生的事件，如果是2，表示留存事件，如果是0，表示不相关的记录，如果是3，表示首次事件和留存事件是一样的。
2. 代码中数据结构说明:

```
struct RetentionInfoAgg {
    int first_array[array_size];  // 用于存储首次事件在该时间单元里边(天，周,月)最早发生的时间
    int second_array[array_size]; // 用于存储留存事件在该时间单元里边(天，周，月)最晚发生的时间
    bool same_event = false; // 判断首次事件和留存事件是否是一个事件
};
```
3. Notice
 - 目前粒度主要是计算按天留存，所以将时间戳从毫秒的精度降低为秒的精度
 - 每个用户最终的留存信息是一个矩阵
 - 留存目前支持一百天的查询，最终返回结果也是每个独立用户统计留存信息叠加的矩阵。
 
## 漏斗分析UDAF
1. 函数参数说明:
 - from_time: 查询的开始时间
 - time_window: 漏斗窗口期，在该窗口期内发生的漏斗才视为有效漏斗
 - steps: 漏斗步骤，根据bit编码，例如第3步为100, 十进制表示为4(这里主要是为了兼容历史接口，所以采用了1,2,4,8),
  现实漏斗分析中加上各种限制条件的事件只有一个唯一的步骤，例如漏斗步骤可能为click->open->open->end，但是中间的两个open有不同的限制条件，因此不会归属到相同的步骤中去。
 - event_time: 传入的事件发生时间

2. 代码中数据结构说明:

```
struct Event {
    uinit8_t _step; //事件所属步骤
    int64_t _ts; //事件的时间戳
}

struct FunnelInfoAgg {
    list<Event> _events;    // 用一用户所有事件的集合
    int64_t _time_window;  //漏斗窗口期
}

// 指针中存放的数据结构类型及含义
aggInfo->ptr=[from_time(int64)]->[time_window(int64)]->{[step(uint8_t)][event_time(int64_t)], [step(uint8_t)][event_time(int64)]...}

```

3. Parse函数
将上述aggInfo->ptr数据转为FunnelInfoAgg类型

4. Output函数(漏斗处理步骤)
 - 对FunnelInfoAgg中用户事件进行排序
 - 过滤重复的事件（相同限定条件的事件才被认为是重复的事件），提升运行效率(例如AAAAAAA的事件发生顺序只保留开始与结束AA),如果漏斗顺序为目前不存在AAAA
 - 对处理后的总事件进行计算，获取该用户的漏斗
 — 在每天中，我们只获取用户在指定时间窗口内的一个最大的序列

5. Notice
 - trim作用, 某些用户存在大量重复事件，例如AAAAA，不做过滤会导致每次从第一个A为窗口开始计算，冗余计算影响漏斗性能，因此进行过滤，降低时间复杂度
 - MAX_EVENT_COUNT=5000, 一个用户的记录超过5000条时视为异常用户，为避免异常用户的计算拖慢漏斗性能，将异常的用户进行过滤
 - 漏斗目前支持一百天的查询。
    
