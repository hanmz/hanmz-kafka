package com.hanmz.kafka.test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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

        System.out.println(fetchRecordByOffset("localhost:9092", new TopicPartition("hanmz-test-topic", 10), 7732));
    }

    /**
     * 根据时间戳查询
     *
     * @param bootstrapServers            broker地址
     * @param topicPartitionTimestampList 主题-分区-时间戳
     * @param singlePartitionRecordNumber 单分区最大查询消息条数
     * @return 主题分区-消息列表
     */
    private static Map<TopicPartition, List<ConsumerRecord<Object, Object>>> fetchByTimestamp(String bootstrapServers, List<TopicPartitionTimestamp> topicPartitionTimestampList, int singlePartitionRecordNumber) {

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

            return fetchByOffset(bootstrapServers, topicPartitionOffsetList, singlePartitionRecordNumber);
        }
    }

    /**
     * 根据offset查询消息
     *
     * @param bootstrapServers            broker地址
     * @param topicPartitionOffsetList    topic-partition-offset
     * @param singlePartitionRecordNumber 单分区最大查询消息条数
     * @return 主题分区-消息列表
     */
    private static Map<TopicPartition, List<ConsumerRecord<Object, Object>>> fetchByOffset(String bootstrapServers, List<TopicPartitionOffset> topicPartitionOffsetList, int singlePartitionRecordNumber) {

        try (KafkaConsumer<Object, Object> consumer = createConsumer(bootstrapServers, false)) {

            Map<TopicPartition, List<ConsumerRecord<Object, Object>>> topicPartitionRecordListMap = Maps.newHashMap();

            Map<TopicPartition, Long> currentOffsets = Maps.newHashMap();

            // 待消费的主题分区列表
            List<TopicPartition> topicPartitions = Lists.newLinkedList();
            for (TopicPartitionOffset topicPartitionOffset : topicPartitionOffsetList) {
                topicPartitions.add(topicPartitionOffset.topicPartition());
                currentOffsets.put(topicPartitionOffset.topicPartition(), topicPartitionOffset.offset);
            }

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);

            while (!Thread.interrupted()) {

                currentOffsets.forEach((tp, offset) -> {
                    // 如果拉取到了消息结尾，不再尝试从该分区取消息，注意end offset是即将到来的消息（如果没有消息endOffset为0），所以这里需要用>=比较
                    if (offset >= endOffsets.get(tp)) {
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


    /**
     * 根据主题分区的具体位点查询消息
     *
     * @param bootstrapServers broker地址
     * @param topicPartition   主题-分区
     * @param offset           查询位点
     * @return 位点对应的消息，如果消息不存在，返回null
     */
    private static ConsumerRecord<Object, Object> fetchRecordByOffset(String bootstrapServers, TopicPartition topicPartition, long offset) {
        KafkaConsumer<Object, Object> consumer = createConsumer(bootstrapServers, true);
        consumer.assign(Lists.newArrayList(topicPartition));

        consumer.seek(topicPartition, offset);

        while (!Thread.interrupted()) {
            // 如果超出范围，直接返回null
            long endOffset = consumer.endOffsets(Lists.newArrayList(topicPartition)).getOrDefault(topicPartition, -1L);
            long beginningOffset = consumer.beginningOffsets(Lists.newArrayList(topicPartition)).getOrDefault(topicPartition, -1L);
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
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 10 * 1024 * 1024);

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

    @AllArgsConstructor
    private static class TopicPartitionOffset {
        private final String topic;
        private final int partition;
        private final long offset;

        private TopicPartition topicPartition() {
            return new TopicPartition(topic, partition);
        }
    }

    @AllArgsConstructor
    private static class TopicPartitionTimestamp {
        private final String topic;
        private final int partition;
        private final long timestamp;

        private TopicPartition topicPartition() {
            return new TopicPartition(topic, partition);
        }

    }

}
