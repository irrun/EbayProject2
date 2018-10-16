####背景描述：
一个大型电商网站后面有不同种类的应用(application)支撑不同业务场景。假设共有400组application分布在若干个不同的数据中心(datacenter)的10000台主机(host)上运行，每台主机上有一个agent每10秒会向监控系统发送如下格式的指标(metric)，假定每次发送指标不超过50个。Input Sample：
```java
{
    "metrics": [
        {
            "values": 46.0,
            "name": "cpuUsage",
            "timestamp": "2017-06-02T04:41:10.383Z"
        },
        {
            "values": 102400000,
            "name": "availableMemoryInBytes",
            "timestamp": "2017-06-02T04:41:10.384Z"
        }
        {
            "values": 500000,
            "name": "searchCount",
            "timestamp": "2017-06-02T04:41:10.384Z"
        }
        ......
    ],
    "dimensions": 
    {
        "datacenter": "slc",
        "application": "search",
        "host": "34612-slc.ebay.com"
    }
}
```

**问题描述**：现在请你设计监控系统的子处理系统(processing system)以满足诸如以下的用户案例：
1. 用户可以查询最近1分钟搜索业务(search application)在slc数据中心(datacenter)searchCount的总次数
2. 用户可以查询最近1分钟搜索业务(search application)在slc数据中心(datacenter)95分位(95 percentile)的cpu使用率

**系统需求如下**：
1. Real time processing(实时处理)
2. Aggregate(维度聚合)
- 可以支持以application级别的和datacenter的维度的聚合处理
- 可以支持以时间维度的聚合处理Query(实时查询)

**注**：
1. 可以使用任意open source(开源)流式处理框架
2. 设计的处理系统能同时处理所有agent上报的指标(metric)10000*50=500k/second
3. 高可用性。为简单起见，假设本处理系统会被部署在一个永远不会down的数据中心中，但是系统仍然需要能有健壮性(Robustness)即可以容忍其所在的主机(host)故障。
4. 可以使用任意语言实现
5. 用户使用的查询API/查询语言可以自行定义