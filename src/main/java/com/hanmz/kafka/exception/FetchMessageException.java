package com.hanmz.kafka.exception;

public class FetchMessageException extends RuntimeException {
    public FetchMessageException(String msg) {
        super(msg);
    }
}
