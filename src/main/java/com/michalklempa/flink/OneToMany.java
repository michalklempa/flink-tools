package com.michalklempa.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class OneToMany implements Serializable {

    public static <ONE, MANY> DataStream<Envelope<ONE, MANY>> create(FlatMapFunction<ONE, MANY> unnestFunction, DataStream<ONE> inputStream) {
        return inputStream.flatMap(new FlatMapFunction<ONE, Envelope<ONE, MANY>>() {
            @Override
            public void flatMap(ONE one, Collector<Envelope<ONE, MANY>> out) throws Exception {
                List<MANY> materializedCollect = new ArrayList<>();
                unnestFunction.flatMap(one, new Collector<MANY>() {
                    @Override
                    public void collect(MANY many) {
                        materializedCollect.add(many);
                    }

                    @Override
                    public void close() {
                    }
                });
                for (MANY many : materializedCollect) {
                    out.collect(new Envelope<>(one, many, materializedCollect.size()));
                }
            }
        });
    }
}

