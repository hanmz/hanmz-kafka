package com.hanmz.kafka.test;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import com.hanmz.kafka.AppConfig;
import com.hanmz.kafka.util.DataUtil;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.logging.log4j.util.Strings;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Slf4j
public class Consumer {
    // https://metrics.dropwizard.io/2.2.0/getting-started/ [yammer文档]
    private static final Meter meter = Metrics.newMeter(Consumer.class, "consume", "consume", TimeUnit.SECONDS);

    public static void main(String[] args) {
        run();
    }

    public static void run() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KAFKA-GROUP");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // 重置offset到日志的开始位置
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 单次拉取的最大数据量【16M】
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 16 * 1024 * 1024);
        // 单次最多拉取记录数量【1w】
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10 * 1000);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BlankDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BlankDeserializer.class.getName());

        if (Strings.isNotBlank(AppConfig.saslJaasConfig())) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
            props.put(SaslConfigs.SASL_JAAS_CONFIG, AppConfig.saslJaasConfig());
        }

        // 创建消费者实例
        KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(props);

        // 指定分区消费
        List<TopicPartition> topicPartitions = Lists.newLinkedList();
        topicPartitions.add(new TopicPartition(AppConfig.topic(), AppConfig.getPartition()));
        consumer.assign(Lists.newArrayList(topicPartitions));

        // 设置开始消费的位置
        if (AppConfig.isSeekToBeginning()) {
            consumer.seekToBeginning(topicPartitions);
        } else if (AppConfig.isSeekToEnd()) {
            consumer.seekToEnd(topicPartitions);
        }

        new Thread(() -> {
            while (!Thread.interrupted()) {
                log.info("平均消费速率/s: {}", DataUtil.format(meter.meanRate()));
                log.info("最近1min消费速率: {}", DataUtil.format(meter.oneMinuteRate()));
                log.info("最近5min消费速率: {}", DataUtil.format(meter.fiveMinuteRate()));
                log.info("最近15min消费速率: {}", DataUtil.format(meter.fifteenMinuteRate()));
                log.info("-----------------------------");
                Uninterruptibles.sleepUninterruptibly(AppConfig.printInterval(), TimeUnit.SECONDS);
            }
        }).start();

        while (!Thread.interrupted()) {
            for (ConsumerRecord record : consumer.poll(500)) {
                meter.mark();
//                log.info("partition: {}, offset: {}, key: {}, value: {}", record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }
}
