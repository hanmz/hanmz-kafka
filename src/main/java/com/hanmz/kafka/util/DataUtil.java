package com.hanmz.kafka.util;

import java.text.DecimalFormat;

/**
 * @author hanmz
 * @date 15/6/2020
 */
public class DataUtil {

    public static void main(String[] args) {
        System.out.println(format(10.00));
        System.out.println(format(10.001));
        System.out.println(format(00.01));
        System.out.println(format(00.001));
    }

    /**
     * 保留两位小数，非四舍五入
     */
    public static String format(double d) {
        DecimalFormat decimalFormat = new DecimalFormat("0.##");

        return decimalFormat.format(d);
    }

    /**
     * 保留两位小数，四舍五入
     */
    public static String formatRound(double d) {

        DecimalFormat decimalFormat = new DecimalFormat("0.##");

        return decimalFormat.format((double) Math.round(d * 100) / 100);
    }

    /**
     * 保留三位小数，非四舍五入
     */
    public static String format3(double d) {
        DecimalFormat decimalFormat = new DecimalFormat("0.###");

        return decimalFormat.format(d);
    }

    /**
     * 保留三位小数，非四舍五入
     */
    public static String format4(double d) {
        DecimalFormat decimalFormat = new DecimalFormat("0.####");

        return decimalFormat.format(d);
    }
}
