## [第一届POLARDB数据性能大赛](https://tianchi.aliyun.com/programming/introduction.htm)

思路：
1. 写阶段：参考badger，分区，DIO，异步队列， 合并批量写
2. 读阶段：分区，DIO，随机读
3. range：合并range请求，DIO，线程池随机读，IO线程不做任何copy操作

结果：
1. 初赛还算可以 240s，虽然跟大佬差的很远，但是还能看
2. 复赛最后时刻才优化到关键点，跑的很慢