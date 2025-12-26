package com.michalklempa.flink;

import org.apache.flink.api.java.functions.KeySelector;

public class IdentityKeySelector<T> implements KeySelector<T, T> {
    private static final long serialVersionUID = 1322521377L;

    public IdentityKeySelector() {
    }

    public T getKey(T value) throws Exception {
        return value;
    }
}
