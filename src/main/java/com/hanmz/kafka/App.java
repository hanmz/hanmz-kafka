package com.hanmz.kafka;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;

/**
 * 计算预算
 */
@Slf4j
public class App {
    public static void main(String[] args) throws Exception {
        Class<?> clazz = Class.forName(AppConfig.mainClass());
        Method method = clazz.getMethod("run");
        method.invoke(clazz.newInstance());
    }
}
