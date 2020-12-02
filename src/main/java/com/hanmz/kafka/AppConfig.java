package com.hanmz.kafka;

import com.sun.media.jfxmediaimpl.HostUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Objects;
import java.util.Properties;

/**
 * @author hanmz
 */
public class AppConfig {
    private static final Properties conf = new Properties();
    private static final String CHARSET = "UTF-8";

    static {
        try {
            if (HostUtils.isMacOSX()) {
                // 本地配置加载路径，注意，这里要指定编码方式，否者中文会出现乱码
                conf.load(new InputStreamReader(Objects.requireNonNull(AppConfig.class.getClassLoader().getResourceAsStream("conf")), CHARSET));
                System.out.println(conf.toString());
            } else {
                // linux服务器上配置加载路径（同一目录下）
                conf.load(new InputStreamReader(new FileInputStream(new File("./conf")), CHARSET));
            }
        } catch (Exception e) {
            System.out.println("加载配置文件失败，进程退出");
            System.exit(-1);
        }
    }

    public static String mainClass() {
        return conf.getProperty("mainClass");
    }

    public static String topic() {
        return conf.getProperty("topic");
    }

    public static String bootstrapServers() {
        return conf.getProperty("bootstrapServers", "").trim();
    }

    public static int getPartition() {
        return NumberUtils.toInt(conf.getProperty("partition", "1").trim());
    }

    public static boolean isSeekToBeginning() {
        return BooleanUtils.toBoolean(conf.getProperty("seekToBeginning", "true").trim());
    }

    public static boolean isSeekToEnd() {
        return BooleanUtils.toBoolean(conf.getProperty("seekToEnd", "false").trim());
    }

    public static String ack() {
        return conf.getProperty("ack", "1").trim();
    }

    public static String saslJaasConfig() {
        return conf.getProperty("saslJaasConfig", "").trim();
    }

    public static int msgSize() {
        return NumberUtils.toInt(conf.getProperty("msgSize", "128").trim());
    }

    public static int printInterval() {
        return NumberUtils.toInt(conf.getProperty("printInterval", "2").trim());
    }
}
