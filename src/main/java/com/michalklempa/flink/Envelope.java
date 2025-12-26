package com.michalklempa.flink;

import java.util.Objects;

public class Envelope<ONE, MANY> {
    public ONE one;
    public MANY many;
    public Integer manyCount;

    public Envelope() {
    }

    public Envelope(ONE one, MANY many, Integer manyCount) {
        this.one = one;
        this.many = many;
        this.manyCount = manyCount;
    }

    public ONE getOne() {
        return one;
    }

    public void setOne(ONE one) {
        this.one = one;
    }

    public MANY getMany() {
        return many;
    }

    public void setMany(MANY many) {
        this.many = many;
    }

    public Integer getManyCount() {
        return manyCount;
    }

    public void setManyCount(Integer manyCount) {
        this.manyCount = manyCount;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Envelope<?, ?> envelope = (Envelope<?, ?>) o;
        return Objects.equals(one, envelope.one) && Objects.equals(many, envelope.many) && Objects.equals(manyCount, envelope.manyCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(one, many, manyCount);
    }

    @Override
    public String toString() {
        return "{\"type\":\"Envelope\","
                + "\"one\":" + one
                + ", \"many\":" + many
                + ", \"manyCount\":\"" + manyCount + "\""
                + "}";
    }
}
