##  图解 Kafka 之实战指南 学习笔记

### chapter_04
- 序列化器: 生产者需要用序列化器（Serializer）把对象转换成字节数组才能通过网络发送给 Kafka。
而在对侧，消费者需要用反序列化器（Deserializer）把从 Kafka 中收到的字节数组转换成相应的对象


### chapter_03

配置信息以KafkaConfig类中注释为准，包括key, value的序列化和反序列化; 集群地址; 客户端ID; 消息发送阻塞时间和重试次数

Kafka消息对象
![](image/ProducerRecord.png)

分别写了同步发送和异步发送消息的方法，其中同步是采用Future的get阻塞调用，异步是添加了回调方法处理的

注意close()方法是在阻塞等待之前所有的发送请求完成后再关闭

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

--- 
配置文件中的参数

- zookeeper.connect: 该参数指明 broker 要连接的 ZooKeeper 集群的服务地址（包含端口号），没有默认值，且此参数为必填项
- listeners: 该参数指明 broker 监听客户端连接的地址列表，即为客户端要连接 broker 的入口地址列表，
  protocol1://hostname1:port1,protocol2://hostname2:port2(协议://主机名:port，多个以逗号隔开)
- advertised.listeners: 公有云上的机器通常配备有多块网卡，即包含私网网卡和公网网卡，对于这种情况而言，
  可以设置 advertised.listeners 参数绑定公网IP供外部客户端使用，而配置 listeners 参数来绑定私网IP地址供 broker 间通信使用。
- broker.id: 指定 Kafka 集群中 broker 的唯一标识
- log.dir和log.dirs: Kafka 把所有的消息都保存在磁盘上，而这两个参数用来配置 Kafka 日志文件存放的根目录。一般情况下，log.dir 用来配置单个根目录，
  而 log.dirs 用来配置多个根目录（以逗号分隔），但是 Kafka 并没有对此做强制性限制，也就是说，log.dir 和 log.dirs 都可以用来配置单个或多个根目录。
  log.dirs 的优先级比 log.dir 高，但是如果没有配置 log.dirs，则会以 log.dir 配置为准。默认情况下只配置了 log.dir 参数，其默认值为 /tmp/kafka-logs。
- message.max.bytes: 该参数用来指定 broker 所能接收消息的最大值，默认值为1000012（B），约等于976.6KB。
  如果 Producer 发送的消息大于这个参数所设置的值，那么（Producer）就会报出 RecordTooLargeException 的异常。
  如果需要修改这个参数，那么还要考虑 max.request.size（客户端参数）、max.message.bytes（topic端参数）等参数的影响。
  为了避免修改此参数而引起级联的影响，建议在修改此参数之前考虑分拆消息的可行性。

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

![img.png](image/chapter_01/img_4.png)

2. kafka中有主题和分区的概念。Kafka中的消息是以主题为单位进行归类的，生产者负责将消息发送到具体的主题，消费者负责订阅主题并进行消费。
   每一个主题可以分为多个分区（也可以称为是主题分区），分区其实在存储层面对应的是一个log文件，文件中会记录偏移量（offset）来保证消息在分区内的顺序。
   如下图所示，一个主题有三个分区，消息会被追加到每个分区的log文件的尾部。每一条消息被发送到broker之前，会根据分区规则存储到具体的分区，
   如果主题只有一个分区的话，那么这个文件在服务器上的log文件I/O将成为性能的瓶颈，合理指定分区数则能解决这个问题。

![img_1.png](image/chapter_01/img_3.png)

3. 分区可以分布在不同的broker上，也就是说，一个主题的多个分区可以横跨多个broker。同时kafka的分区具有多副本机制，通过增加副本的数量来增加容灾能力。
   同一个分区的不同副本中保存的是相同的消息（follower副本完成消息同步之后），副本与副本之间是一主多从的关系，其中只有leader副本负责读写，
   而follower副本只负责从leader副本处进行消息同步。如下图所示，一个主题有三个分区，副本因子也为三（即每个分区的副本个数为三，
   那么每个分区就有一个leader副本和两个follower副本），均匀的分布在三个broker上。

![img_2.png](image/chapter_01/img_2.png)

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