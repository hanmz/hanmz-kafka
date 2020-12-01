package com.hanmz.kafka.test;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * 自定义空白反序列化类
 * 不对消息key、value反序列化
 */
public class BlankDeserializer implements Deserializer {
    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        return null;
    }

    @Override
    public void close() {

    }
}
