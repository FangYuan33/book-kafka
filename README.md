##  图解 Kafka 之实战指南 学习笔记

### chapter_06

- 重要的生产者参数

1. **acks** 这个参数指定分区中必须要有多少个副本收到这条消息，生产者才认为这条消息是成功写入的。

**默认值为1**，即生产者发送消息后，只要分区leader副本成功写入消息，那么它就会收到来自服务器的成功响应。如果消息写入leader副本成功，但是在被follower
   副本拉取之前leader崩溃，那么这条消息还是会丢失。它是消息可靠性和吞吐量之间的折中方案。
   
**当acks = 0时**，生产者发送消息之后不需要等待任何服务器的响应就认定发送成功。这是吞吐量最大的方案。

**当acks = -1或acks = all时**，生产者发送消息之后，需要等待ISR中所有副本写入才能收到来自服务器的成功响应。它是可靠性最高的方案，
但是也并不意味着消息就一定可靠，因为如果ISR中只有leader一个那么它其实和配置ack = 1的效果是一样的。
   
2. **max.request.size**: 这个参数用来限制生产者客户端能发送消息的最大值，默认为1048576B，即1MB。这个参数会涉及与 **message.max.bytes**
参数的联动，如果将 message.max.bytes配置为10而max.request.size配置为20，这时候发送一条15b的消息，就会出现异常。
   
> org.apache.kafka.common.errors.RecordTooLargeException: The request included a message larger than the max message size the server will accept.

3. **max.in.flight.requests.per.connection**: 限制每个连接（也就是客户端与 Node 之间的连接）最多缓存的请求数，在需要保证消息顺序的场景下
建议把这个参数配置为1，如果这个参数大于1且失败重试次数非零时，会出现错序的现象：第一批次写入失败，第二批次写入成功，第一批次重试写入成功后则会发生错序。
   
4. **retries和retry.backoff.ms**: 前者用来配置生产者发送消息失败后重试的次数，后者用来配置两次重试间的时间间隔，尽可能避免无效重试，也可以估算
一下异常恢复的时间，合理配置时间间隔来避免生产者过早的放弃重试。像网络波动和leader副本选举发生时，通过重试是能将消息发送成功的。
   
5. **compression.type**: 这个参数用来指定消息的压缩方式，默认值为“none”，即默认情况下，消息不会被压缩。
   该参数还可以配置为“gzip”“snappy”和“lz4”。对消息进行压缩可以极大地减少网络传输量、降低网络I/O，从而提高整体的性能。
   消息压缩是一种使用时间换空间的优化方式，如果对时延有一定的要求，则不推荐对消息进行压缩。
   
6. **connections.max.idle.ms**: 这个参数用来指定在多久之后关闭闲置的连接，默认值是540000（ms），即9分钟。

7. linger.ms: 这个参数用来指定生产者发送 ProducerBatch 之前等待更多消息（ProducerRecord）加入 ProducerBatch 的时间，默认值为0。 
生产者客户端会在 ProducerBatch 被填满或等待时间超过 linger.ms 值时发送出去。增大这个参数的值会增加消息的延迟，但是同时能提升一定的吞吐量。

8. receive.buffer.bytes: 这个参数用来设置 Socket 接收消息缓冲区（SO_RECBUF）的大小，默认值为32768（B），即32KB。
如果设置为-1，则使用操作系统的默认值。如果 Producer 与 Kafka 处于不同的机房，则可以适地调大这个参数值。

9. send.buffer.bytes: 这个参数用来设置 Socket 发送消息缓冲区（SO_SNDBUF）的大小，默认值为131072（B），即128KB。
与 receive.buffer.bytes 参数一样，如果设置为-1，则使用操作系统的默认值。

10. request.timeout.ms: 这个参数用来配置 Producer 等待请求响应的最长时间，默认值为30000（ms）。请求超时之后可以选择进行重试。
注意这个参数需要比 broker 端参数 replica.lag.time.max.ms 的值要大，这样可以减少因客户端重试而引起的消息重复的概率。

11. enable.idempotence: 是否开启幂等性功能

### chapter_05

- 生产者客户端的整体架构

![img.png](image/chapter_05/img.png)

整个生产者客户端由两个线程协调运行，这两个线程分别为主线程和 Sender 线程（发送线程）。在主线程中由 KafkaProducer 创建消息，
然后通过可能的拦截器、序列化器和分区器的作用之后缓存到消息累加器（RecordAccumulator，也称为消息收集器）中。

Sender 线程负责从 RecordAccumulator 中获取消息并将其发送到 Kafka 中。RecordAccumulator 缓存的大小可以通过生产者客户端参数 buffer.memory 配置，默认值为 33554432B，即32MB。

RecordAccumulator 主要用来缓存消息以便 Sender 线程可以批量发送，进而减少网络传输的资源消耗以提升性能。

如果生产者发送消息的速度超过发送到服务器的速度，则会导致生产者空间不足，这个时候 KafkaProducer 的 send() 方法调用要么被阻塞，
要么抛出异常，这个取决于参数 max.block.ms 的配置，此参数的默认值为60000，即60秒。

![img_1.png](image/chapter_05/img_1.png)

在 RecordAccumulator 的内部为每个分区都维护了一个双端队列，队列中的内容是ProducerBatch，消息写入缓存时，追加到双端队列的尾部；Sender 读取消息时，从双端队列的头部读取。

![](image/chapter_05/ProducerBatch.jpg)

ProducerBatch 中可以包含一至多个 ProducerRecord，
ProducerRecord 是生产者中创建的消息，而 ProducerBatch 是指一个消息批次，ProducerRecord 会被包含在 ProducerBatch 中，
这样可以使字节的使用更加紧凑。与此同时，将较小的 ProducerRecord 拼凑成一个较大的 ProducerBatch，也可以减少网络请求的次数以提升整体的吞吐量。

消息在网络上都是以字节（Byte）的形式传输的，在发送之前需要创建一块内存区域来保存对应的消息。这一点我们在定义序列化器和反序列化器时可以发现。

在 Kafka 生产者客户端中，通过 ByteBuffer 实现消息内存的创建和释放。 不过频繁的创建和释放是比较耗费资源的，
在 RecordAccumulator 的内部还有一个 BufferPool，它主要用来实现 ByteBuffer 的复用，以实现缓存的高效利用。

不过 BufferPool 只针对特定大小的 ByteBuffer 进行管理，而其他大小的 ByteBuffer 不会缓存进 BufferPool 中，
这个特定的大小由 batch.size 参数来指定，默认值为16384B，即16KB。我们可以适当地调大 batch.size 参数以便多缓存一些消息。

当一条消息（ProducerRecord）流入 RecordAccumulator 时，会先寻找与消息分区所对应的双端队列（如果没有则新建），
再从这个双端队列的尾部获取一个 ProducerBatch（如果没有则新建），查看 ProducerBatch 中是否还可以写入这个 ProducerRecord，如果可以则写入，
如果不可以则需要创建一个新的 ProducerBatch。在新建 ProducerBatch 时评估这条消息的大小是否超过 batch.size 参数的大小，
如果不超过，那么就以 batch.size 参数的大小来创建 ProducerBatch，这样在使用完这段内存区域之后，可以通过 BufferPool 的管理来进行复用；
如果超过，那么就以评估的大小来创建 ProducerBatch，这段内存区域不会被复用。

![](image/chapter_05/sender.jpg)

Sender 从 RecordAccumulator 中获取缓存的消息之后，会进行如上图所示的数据类型转换，由先前的`<分区, Deque<ProducerBatch>>`类型转换成
`<Node, List< ProducerBatch>>`类型，其中Node为broker节点，相当于是做了从应用层到网络I/O层的转换，因为对于网络链接来说，它只关系需要
发送的目的broker节点，并不关心应用层消息所在的分区。

![](image/chapter_05/sender2.jpg)

Sender之后还会对他进行一次封装，封装成<Node, Request>的形式，这样就更直观的展示了将Request发往各个Node的逻辑。

![img_2.png](image/chapter_05/img_2.png)

请求在从 Sender 线程发往 Kafka 之前还会保存到 InFlightRequests 中，InFlightRequests 保存对象的具体形式为 `Map<NodeId, Deque<InFlightRequest>>`，
它的主要作用是缓存了已经发出去但还没有收到响应的请求（NodeId 是一个 String 类型，表示节点的 id 编号）

对应的InFlightRequests注释
> The set of requests which have been sent or are being sent but haven't yet received a response

通过配置max.in.flight.requests. per. connection，默认为5，可以配置最多缓存的请求数。如果超过了这个数量，就不能再向这个连接发送更多的请求了。

---
- 元数据的更新

![img_3.png](image/chapter_05/img_3.png)

在`InFlightRequests`中还能获取到`leastLoadedNode`，即未确认请求数最少的Node。那么选择该Node发送请求则可以尽快发出，这个特性可以应用在元数据请求时。

为什么要更新元数据？

比如我们要发送这条消息，但是这条消息除了知道主题以外，其他信息一概不知。
```java
ProducerRecord<String, String> record = new ProducerRecord<>(topic, "Hello, Kafka!");
```
而实际上KafkaProducer需要将这条消息追加到**指定主题的所在的某个分区的leader副本**之下，因此我们就需要获取到 **“足够多的元数据信息”** 来达到这一点。

元数据都包含了啥呢？集群中用哪些节点、有哪些主题，这些主题有哪些分区，
每个分区的leader副本在哪个broker节点上，follower副本在哪些节点上，哪些副本在AR、ISR等集合中...

那该如何获取这些元数据呢？

我们在bootstrap.servers 参数上配置了broker节点地址，Sender线程会挑选出`leastLoadedNode`发送向其获取元数据的请求，这个过程是在客户端内部进行的，
对使用者不可见，请求完成后便会获取到需要的元数据信息。超过 `metadata.max.age.ms` 时间没有更新元数据都会引起元数据的更新操作，默认为5分钟

### chapter_04
消息在通过 send() 方法发往 broker 的过程中，有可能需要经过拦截器（Interceptor）、序列化器（Serializer）和
分区器（Partitioner）的一系列作用之后才能被真正地发往 broker。

- 序列化器和反序列化器: 生产者需要用序列化器（Serializer）把对象转换成字节数组才能通过网络发送给 Kafka。
而在对侧，消费者需要用反序列化器（Deserializer）把从 Kafka 中收到的字节数组转换成相应的对象

- 分区器: 消息经过序列化之后就需要确定它发往的分区，如果消息 ProducerRecord 中指定了 partition 字段，那么就不需要分区器的作用(自己写了一个简单分区器)

- 生产者拦截器: 注意看一下MyProducerInterceptor的方法注释

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
