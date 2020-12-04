package com.hanmz.kafka.util;


import com.hanmz.kafka.exception.BusinessException;

public class AssertUtil {

    public static void assertTrue(Boolean isTrue, String errorMessage){
        if(!isTrue){
            throw new BusinessException(errorMessage);
        }
    }

    public static void throwIfTrue(Boolean condition, String errorMessage){
        if(condition){
            throw new BusinessException(errorMessage);
        }
    }

    public static void throwIfNotBetween(Number param, Number start,Number end,String errorMessage){
        if(param.longValue() < start.longValue() || param.longValue() > end.longValue()){
            throw new BusinessException(errorMessage);
        }
    }

    public static void throwIfGreaterThan(Number param, Number expect,String errorMessage){
        if(param.longValue() > expect.longValue()){
            throw new BusinessException(errorMessage);
        }
    }
    public static void throwIfNoEqual(Number param, Number expect,String errorMessage){
        if(param != null && !param.equals(expect)){
            throw new BusinessException(errorMessage);
        }
        if(param == null && expect != null){
            throw new BusinessException(errorMessage);
        }
    }
}
