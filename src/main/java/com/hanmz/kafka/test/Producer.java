package com.hanmz.kafka.test;

import com.hanmz.kafka.App;
import com.hanmz.kafka.AppConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * kafka生产者实例
 */
@Slf4j
public class Producer {
    public static void main(String[] args) throws Exception {
        send();
    }

    private static void send() throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, AppConfig.ack());
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        StringBuilder sbd = new StringBuilder(AppConfig.msgSize());
        for (int i = 0; i < AppConfig.msgSize(); i++) {
            sbd.append("a");
        }
        String msg = sbd.toString();

        while (!Thread.interrupted()) {
            // TODO: hanmz 2020/12/1 同步发送
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>(AppConfig.getTopic(), msg));
            RecordMetadata metadata = future.get();
            log.info("发送成功, 分区：{}, offset: {}", metadata.partition(), metadata.offset());
        }
    }
}
