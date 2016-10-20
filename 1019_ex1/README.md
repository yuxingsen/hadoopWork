# 这是10.19的第一个练习

## 问题描述—Secondary Sort

对每年的气温，输出每年的气温，按照温度从高到低输出

利用课程学习过的hdfs和map reduce技术，实现Secondary Sort功能，要求：
* 利用setPartitionerClass对key进行分区
* 利用setGroupingComparatorClass，进行比较(compare)，从而进一步 完成排序(sort) z——选做，在hadoop 2.6上实现
* 直接调用MapReduce java API或Streaming，实现上述功能
* 尽可能对MR job的进行优化，减少中间数据传输量和计算量
