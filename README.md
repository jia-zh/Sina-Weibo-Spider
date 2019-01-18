# Sina Weibo Spider
基于WebCollector的新浪微博爬虫，以MySQL作为持久化存储。

采用WebCollector的 Berkeley DB（BreadthCrawler)的插件：适合处理长期和大量级的任务，并具有断点爬取功能，不会因为宕机、关闭导致数据丢失。

爬取新浪微的用户信息（包括基本信息、用户标签、用户粉丝、用户关注者）和用户微博信息（包括评论者和转发至）。

对于微博登录cookie，需要提前获取大量用户的cookie，存储在项目根目录的cookies文件中，程序进行动态调用切换。对于微博账号，可以直接从某宝上购买，对于账号的cookie获取，可以参考项目中的WeiboCN.java文件的方式。

对于代理IP的获取，本项目采用开源程序IPProxyPool（ https://github.com/jia-zh/IPProxyPool ），程序进行动态调用切换。

# Requirements
JDK，WebCollector，MySQL

对于IPProxyPool的相关需求，请参考对于的开源项目。

# Attention
1、由于微博中含有4个字节的符号，需要将MySQL的编码格式设置为utf8mb4。

2、由于在持久化的过程中需要一定的内存，需要采用-Xms，-Xmx设置堆内存。
