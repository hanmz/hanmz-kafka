package com.hanmz.kafka.test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.hanmz.kafka.App;
import com.hanmz.kafka.AppConfig;
import com.hanmz.kafka.exception.FetchMessageException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Slf4j
public class FetchMessage {

    public static void main(String[] args) {
//        List<TopicPartitionOffset> topicPartitionOffsetList = Lists.newArrayList(new TopicPartitionOffset("hanmz-test-topic", 1, 0));
//        Map<TopicPartition, List<ConsumerRecord<Object, Object>>> map = fetchByOffset(topicPartitionOffsetList, 20);
//
//        List<TopicPartitionTimestamp> topicPartitionOffsetList = Lists.newArrayList(new TopicPartitionTimestamp("hanmz-test-topic", 1, System.currentTimeMillis() - 1000 * 60 * 60));
//        Map<TopicPartition, List<ConsumerRecord<Object, Object>>> map = fetchByTimestamp(topicPartitionOffsetList, 20);
//
//        System.out.println(map);
//        for (Map.Entry<TopicPartition, List<ConsumerRecord<Object, Object>>> entry : map.entrySet()) {
//            System.out.println("分区：" + entry.getKey() + " 消息数：" + entry.getValue().size());
//        }

//        System.out.println(fetchRecordByOffset("localhost:9092", new TopicPartition("hanmz-test-topic", 10), 7732));
    }

    public static void run() {
        try {
            Class<FetchMessage> clazz = FetchMessage.class;
            if ("fetchMessageListByTimestamp".equals(AppConfig.method())) {
                Method method = clazz.getMethod("fetchMessageListByTimestamp", String.class, String.class, int.class, long.class, int.class);
                method.invoke(new FetchMessage(), AppConfig.bootstrapServers(), AppConfig.topic(), AppConfig.partition(), 0, AppConfig.singlePartitionRecordNumber());
            } else if ("fetchMessageListByOffset".equals(AppConfig.method())) {
                Method method = clazz.getMethod("fetchMessageListByOffset", String.class, String.class, int.class, long.class, int.class);
                method.invoke(new FetchMessage(), AppConfig.bootstrapServers(), AppConfig.topic(), AppConfig.partition(), AppConfig.offset(), AppConfig.singlePartitionRecordNumber());
            } else if ("fetchMessageByOffset".equals(AppConfig.method())) {
                Method method = clazz.getMethod("fetchMessageByOffset", String.class, String.class, int.class, long.class);
                method.invoke(new FetchMessage(), AppConfig.bootstrapServers(), AppConfig.topic(), AppConfig.partition(), AppConfig.offset());
            } else {
                log.error("Unknown method");
                System.exit(-1);
            }
        } catch (Exception e) {
            log.info("error", e);
        }
    }


    public List<ConsumerRecord<Object, Object>> fetchMessageListByTimestamp(String bootstrapServers, String topic, int partition, long timestamp, int singlePartitionRecordNumber) throws Exception {

        verify(bootstrapServers, topic, partition);

        Map<TopicPartition, List<ConsumerRecord<Object, Object>>> map = fetchMessageListByTimestamp(bootstrapServers, Lists.newArrayList(new TopicPartitionTimestamp(topic, partition, timestamp)), singlePartitionRecordNumber);
        return map.get(new TopicPartition(topic, partition));
    }

    /**
     * 根据时间戳查询消息列表
     *
     * @param bootstrapServers            broker地址
     * @param topicPartitionTimestampList 主题-分区-时间戳
     * @param singlePartitionRecordNumber 单分区最大查询消息条数
     * @return 主题分区-消息列表
     */
    public Map<TopicPartition, List<ConsumerRecord<Object, Object>>> fetchMessageListByTimestamp(String bootstrapServers, List<TopicPartitionTimestamp> topicPartitionTimestampList, int singlePartitionRecordNumber) {

        try (KafkaConsumer<Object, Object> consumer = createConsumer(bootstrapServers, false)) {

            Map<TopicPartition, Long> timestampsToSearch = Maps.newHashMap();
            List<TopicPartition> topicPartitions = Lists.newLinkedList();
            topicPartitionTimestampList.forEach(tpt -> {
                topicPartitions.add(tpt.topicPartition());
                timestampsToSearch.put(tpt.topicPartition(), tpt.timestamp);
            });

            // 指定分区
            consumer.assign(topicPartitions);

            List<TopicPartitionOffset> topicPartitionOffsetList = Lists.newArrayList();
            Map<TopicPartition, OffsetAndTimestamp> offsetsTimes = consumer.offsetsForTimes(timestampsToSearch);
            for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetsTimes.entrySet()) {
                // 指定timestamp后没有消息
                if (entry.getValue() == null) {
                    return Maps.newHashMap();
                }
                topicPartitionOffsetList.add(new TopicPartitionOffset(entry.getKey().topic(), entry.getKey().partition(), entry.getValue().offset()));
            }

            // 解除绑定
            consumer.unsubscribe();

            return fetchMessageListByOffset(bootstrapServers, topicPartitionOffsetList, singlePartitionRecordNumber);
        }
    }

    public List<ConsumerRecord<Object, Object>> fetchMessageListByOffset(String bootstrapServers, String topic, int partition, long offset, int singlePartitionRecordNumber) throws Exception {

        verify(bootstrapServers, topic, partition);

        Map<TopicPartition, List<ConsumerRecord<Object, Object>>> map = fetchMessageListByOffset(bootstrapServers, Lists.newArrayList(new TopicPartitionOffset(topic, partition, offset)), singlePartitionRecordNumber);

        return map.get(new TopicPartition(topic, partition));
    }

    /**
     * 根据offset查询消息列表
     *
     * @param bootstrapServers            broker地址
     * @param topicPartitionOffsetList    topic-partition-offset
     * @param singlePartitionRecordNumber 单分区最大查询消息条数
     * @return 主题分区-消息列表
     */
    public Map<TopicPartition, List<ConsumerRecord<Object, Object>>> fetchMessageListByOffset(String bootstrapServers, List<TopicPartitionOffset> topicPartitionOffsetList, int singlePartitionRecordNumber) {

        try (KafkaConsumer<Object, Object> consumer = createConsumer(bootstrapServers, false)) {

            Map<TopicPartition, List<ConsumerRecord<Object, Object>>> topicPartitionRecordListMap = Maps.newHashMap();

            Map<TopicPartition, Long> currentOffsets = Maps.newHashMap();

            // 待消费的主题分区列表
            List<TopicPartition> topicPartitions = Lists.newLinkedList();
            for (TopicPartitionOffset topicPartitionOffset : topicPartitionOffsetList) {
                topicPartitions.add(topicPartitionOffset.topicPartition());
                // 设置初始拉取位置的offset，初始位置不小于最小offset
                currentOffsets.put(topicPartitionOffset.topicPartition(), Math.max(topicPartitionOffset.offset, consumer.beginningOffsets(topicPartitions).getOrDefault(topicPartitionOffset.topicPartition(), Long.MAX_VALUE)));
            }

            while (!Thread.interrupted()) {

                if (topicPartitions.isEmpty()) {
                    break;
                }

                currentOffsets.forEach((tp, offset) -> {
                    // 如果拉取到了消息结尾，不再尝试从该分区取消息，注意end offset是即将到来的消息（如果没有消息endOffset为0），所以这里需要用>=比较
                    if (offset >= consumer.endOffsets(topicPartitions).getOrDefault(tp, -1L) || offset < consumer.beginningOffsets(topicPartitions).getOrDefault(tp, Long.MAX_VALUE)) {
                        topicPartitions.remove(tp);
                    }
                });

                if (topicPartitions.isEmpty()) {
                    break;
                }

                // 指定消费分区
                consumer.assign(topicPartitions);

                // 指定各个分区拉取消息的offset
                currentOffsets.forEach(consumer::seek);

                Iterator<TopicPartition> iterator = topicPartitions.iterator();
                while (iterator.hasNext()) {
                    TopicPartition topicPartition = iterator.next();
                    for (ConsumerRecord<Object, Object> record : consumer.poll(100).records(topicPartition)) {
                        List<ConsumerRecord<Object, Object>> consumerRecords = topicPartitionRecordListMap.get(topicPartition);
                        if (consumerRecords == null) {
                            topicPartitionRecordListMap.put(topicPartition, Lists.newArrayList(record));
                        } else {
                            consumerRecords.add(record);
                        }

                        // 如果单分区消息量已经满足最大拉去数量要求，不再从该分区中拉取数据
                        if (topicPartitionRecordListMap.get(topicPartition).size() >= singlePartitionRecordNumber) {
                            iterator.remove();
                            break;
                        }

                        // 设置当前拉取消息位置
                        currentOffsets.put(topicPartition, record.offset() + 1);
                    }
                }

                // 解除绑定
                consumer.unsubscribe();
            }

            return topicPartitionRecordListMap;
        }
    }

    public ConsumerRecord<Object, Object> fetchMessageByOffset(String bootstrapServers, String topic, int partition, long offset) throws Exception {

        verify(bootstrapServers, topic, partition);

        return fetchMessageByOffset(bootstrapServers, new TopicPartition(topic, partition), offset);
    }

    /**
     * 根据主题分区的具体位点查询消息
     *
     * @param bootstrapServers broker地址
     * @param topicPartition   主题-分区
     * @param offset           查询位点
     * @return 位点对应的消息，如果消息不存在，返回null
     */
    public ConsumerRecord<Object, Object> fetchMessageByOffset(String bootstrapServers, TopicPartition topicPartition, long offset) {
        try (KafkaConsumer<Object, Object> consumer = createConsumer(bootstrapServers, true)) {
            consumer.assign(Lists.newArrayList(topicPartition));

            consumer.seek(topicPartition, offset);

            while (!Thread.interrupted()) {
                // 如果超出范围，直接返回null
                long endOffset = consumer.endOffsets(Lists.newArrayList(topicPartition)).getOrDefault(topicPartition, -1L);
                long beginningOffset = consumer.beginningOffsets(Lists.newArrayList(topicPartition)).getOrDefault(topicPartition, Long.MAX_VALUE);
                if (offset < beginningOffset || offset >= endOffset) {
                    log.warn("没有找到分区({})在offset({})位置的消息,分区offset范围[{},{}]", topicPartition, offset, beginningOffset, endOffset - 1);
                    break;
                }

                for (ConsumerRecord<Object, Object> record : consumer.poll(5 * 1000L)) {
                    if (record.offset() != offset) {
                        log.warn("没有找到分区({})在offset({})位置的消息", topicPartition, offset);
                        return null;
                    } else {
                        return record;
                    }
                }
            }
        }
        return null;
    }

    /**
     * 创建消费者实例
     *
     * @param bootstrapServers  broker地址
     * @param enableDeserialize 是否需要对消息的key、value进行反序列化
     * @return 消费者实例
     */
    private static KafkaConsumer<Object, Object> createConsumer(String bootstrapServers, boolean enableDeserialize) {
        //config中主要变化是 ZooKeeper 参数被替换了
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "CKAFKA-GROUP");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 根据实例最大的消息长度决定
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 16 * 1024 * 1024);

        if (enableDeserialize) {
            // 只支持String类型消息解析
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        } else {
            // 不做解析消息
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BlankDeserializer.class.getName());
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BlankDeserializer.class.getName());
        }
        return new KafkaConsumer<>(props);
    }

    /**
     * 校验主题和分区是否存在
     */
    void verify(String bootstrapServers, String realTopic, int partition) throws Exception {
        if (partition < 0) {
            throw new FetchMessageException(String.format("Partition [%s] illegal", partition));
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);

        try (AdminClient adminClient = KafkaAdminClient.create(props)) {

            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Lists.newArrayList(realTopic));

            KafkaFuture<TopicDescription> descriptionKafkaFuture = describeTopicsResult.values().get(realTopic);
            TopicDescription description = descriptionKafkaFuture.get(3, TimeUnit.SECONDS);

            int partitionNumber = description.partitions().size();

            if (partition >= partitionNumber) {
                throw new FetchMessageException(String.format("Partition [%s] over max partition [%s]", partition, partitionNumber - 1));
            }
        }
    }

    @AllArgsConstructor
    static class TopicPartitionOffset {
        private final String topic;
        private final int partition;
        private final long offset;

        private TopicPartition topicPartition() {
            return new TopicPartition(topic, partition);
        }
    }

    @AllArgsConstructor
    static class TopicPartitionTimestamp {
        private final String topic;
        private final int partition;
        private final long timestamp;

        private TopicPartition topicPartition() {
            return new TopicPartition(topic, partition);
        }

    }
}
