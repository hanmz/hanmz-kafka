package com.hanmz.kafka.test;

import com.google.common.util.concurrent.Uninterruptibles;
import com.hanmz.kafka.AppConfig;
import com.hanmz.kafka.util.DataUtil;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.util.Strings;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * kafka生产者实例
 */
@Slf4j
public class Producer {
    // https://metrics.dropwizard.io/2.2.0/getting-started/ [yammer文档]
    private static final Meter meter = Metrics.newMeter(Consumer.class, "produce", "produce", TimeUnit.SECONDS);

    public static void main(String[] args) {
        send();
    }

    private static void send() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, AppConfig.ack());
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);

        if (Strings.isNotBlank(AppConfig.saslJaasConfig())) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
            props.put(SaslConfigs.SASL_JAAS_CONFIG, AppConfig.saslJaasConfig());
        }

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        StringBuilder sbd = new StringBuilder(AppConfig.msgSize());
        for (int i = 0; i < AppConfig.msgSize(); i++) {
            sbd.append("a");
        }
        String msg = sbd.toString();

        new Thread(() -> {
            while (!Thread.interrupted()) {
                log.info("平均生产速率/s: {}", DataUtil.format(meter.meanRate()));
                log.info("最近1min生产速率: {}", DataUtil.format(meter.oneMinuteRate()));
                log.info("最近5min生产速率: {}", DataUtil.format(meter.fiveMinuteRate()));
                log.info("最近15min生产速率: {}", DataUtil.format(meter.fifteenMinuteRate()));
                log.info("-----------------------------");
                Uninterruptibles.sleepUninterruptibly(AppConfig.printInterval(), TimeUnit.SECONDS);
            }
        }).start();

        while (!Thread.interrupted()) {
            // TODO: hanmz 2020/12/1 异步发送
            producer.send(new ProducerRecord<>(AppConfig.topic(), msg),
                    (metadata, exception) -> {
                        meter.mark();
//                        log.info("发送成功, 分区：{}, offset: {}", metadata.partition(), metadata.offset());
                    });
        }
    }
}
