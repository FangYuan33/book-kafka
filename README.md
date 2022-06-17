##  图解 Kafka 之实战指南 学习笔记

### chapter_02

在远程Linux上配置
```xml
listeners=PLAINTEXT://内网IP:9092

advertised.listeners=PLAINTEXT://外网IP:9092
```

创建分区为4副本因子为3的话题
```
bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic topic-demo --replication-factor 3 --partitions 4
```

展示主题的更多具体信息
```
bin/kafka-topics.sh --zookeeper localhost:2181 --describe --topic topic-demo
```

通过 kafka-console-consumer.sh 脚本来订阅主题 topic-demo 其中--bootstrap-server 指定了连接的 Kafka 集群地址
```
bin/kafka-console-consumer.sh --bootstrap-server 10.0.24.15:9092 --topic topic-demo
```

使用 kafka-console-producer.sh 脚本发送一条消息“Hello, Kafka!”至主题 topic-demo
```
bin/kafka-console-producer.sh --broker-list 10.0.24.15:9092 --topic topic-demo
> hello Kafka!
```

### chapter_01

Kafka的三大角色

- **消息系统**： Kafka 和传统的消息系统（也称作消息中间件）都具备系统解耦、冗余存储、流量削峰、缓冲、异步通信、扩展性、可恢复性等功能。与此同时，
  Kafka 还提供了大多数消息系统难以实现的**消息顺序性保障**及**回溯消费**的功能。
  
- **存储系统**： Kafka 把消息持久化到磁盘，相比于其他基于内存存储的系统而言，有效地降低了数据丢失的风险。
  也正是得益于 Kafka 的消息持久化功能和多副本机制， 我们可以把 Kafka 作为长期的数据存储系统来使用，
  只需要把对应的数据保留策略设置为“永久”或启用主题的日志压缩功能即可。
  
- **流式处理平台**： Kafka 不仅为每个流行的流式处理框架提供了可靠的数据来源，还提供了一个完整的流式处理类库，比如窗口、连接、变换和聚合等各类操作。


1. kafka集群包含一个或多个服务器，而每个服务器可以简单的被称为broker，生产者producer和消费者consumer是broker的客户端。
   一个典型的Kafka体系包含若干producer、若干broker和若干consumer，producer发送消息到broker，consumer拉取消息进行消费，如下图所示

![在这里插入图片描述](https://img-blog.csdnimg.cn/187e1e38c0d640649bbbc97a19d6b091.png?x-oss-process=image/watermark,type_d3F5LXplbmhlaQ,shadow_50,text_Q1NETiBA5pa55ZyG5oOz5b2T5Zu-54G1,size_14,color_FFFFFF,t_70,g_se,x_16#pic_center)

2. kafka中有主题和分区的概念。Kafka中的消息是以主题为单位进行归类的，生产者负责将消息发送到具体的主题，消费者负责订阅主题并进行消费。
   每一个主题可以分为多个分区（也可以称为是主题分区），分区其实在存储层面对应的是一个log文件，文件中会记录偏移量（offset）来保证消息在分区内的顺序。
   如下图所示，一个主题有三个分区，消息会被追加到每个分区的log文件的尾部。每一条消息被发送到broker之前，会根据分区规则存储到具体的分区，
   如果主题只有一个分区的话，那么这个文件在服务器上的log文件I/O将成为性能的瓶颈，合理指定分区数则能解决这个问题。

![在这里插入图片描述](https://img-blog.csdnimg.cn/6c306798856b4e868a10c2442724f463.png?x-oss-process=image/watermark,type_d3F5LXplbmhlaQ,shadow_50,text_Q1NETiBA5pa55ZyG5oOz5b2T5Zu-54G1,size_16,color_FFFFFF,t_70,g_se,x_16#pic_center)

3. 分区可以分布在不同的broker上，也就是说，一个主题的多个分区可以横跨多个broker。同时kafka的分区具有多副本机制，通过增加副本的数量来增加容灾能力。
   同一个分区的不同副本中保存的是相同的消息（follower副本完成消息同步之后），副本与副本之间是一主多从的关系，其中只有leader副本负责读写，
   而follower副本只负责从leader副本处进行消息同步。如下图所示，一个主题有三个分区，副本因子也为三（即每个分区的副本个数为三，
   那么每个分区就有一个leader副本和两个follower副本），均匀的分布在三个broker上。

![在这里插入图片描述](https://img-blog.csdnimg.cn/5f3914ca92ff47b480b6563d5cddad73.png?x-oss-process=image/watermark,type_d3F5LXplbmhlaQ,shadow_50,text_Q1NETiBA5pa55ZyG5oOz5b2T5Zu-54G1,size_18,color_FFFFFF,t_70,g_se,x_16#pic_center)

4. 当leader副本出现故障时，会从follower中选举出新的leader副本来对外提供服务。分区中的所有副本统称为AR，
   所有与leader副本保持同步的副本被称为ISR，与leader副本同步滞后的被称为OSR。
   
5. 消息会先发送到 leader 副本，然后 follower 副本才能从 leader 副本中拉取消息进行同步，
   同步期间内 follower 副本相对于 leader 副本而言会有一定程度的滞后。滞后范围可以通过参数进行配置。
   leader 副本负责维护和跟踪 ISR 集合中所有 follower 副本的滞后状态，当 follower 副本落后太多或失效时，leader 副本会把它从 ISR 集合中剔除，
   被移动到OSR集合。如果 OSR 集合中有 follower 副本“追上”了 leader 副本，那么 leader 副本会把它从 OSR 集合转移至 ISR 集合。
   默认情况下，当 leader 副本发生故障时，只有在 ISR 集合中的副本才有资格被选举为新的 leader，
   而在 OSR 集合中的副本则没有任何机会（不过这个原则也可以通过修改相应的参数配置来改变）。
   
6. ISR 中 HW和LEO
![img.png](image/chapter_01/img.png)
   
7. 消息写入后，HW和LEO变化图
![img_1.png](image/chapter_01/img_1.png)
   
8. Kafka 的复制机制既不是完全的同步复制，也不是单纯的异步复制。事实上，同步复制要求所有能工作的 follower 副本都复制完，
   这条消息才会被确认为已成功提交，这种复制方式极大地影响了性能。而在异步复制方式下，follower 副本异步地从 leader 副本中复制数据，
   数据只要被 leader 副本写入就被认为已经成功提交。在这种情况下，如果 follower 副本都还没有复制完而落后于 leader 副本，
   突然 leader 副本宕机，则会造成数据丢失。Kafka 使用的这种 ISR 的方式则有效地权衡了数据可靠性和性能之间的关系。