package com.michalklempa.flink;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Topology {
    private static final Logger logger = LoggerFactory.getLogger(Topology.class);
    public DataStream<Order> result;

    public Topology(DataStream<Order> orderDataStream, DataStream<Product> productDataStream) {
        DataStream<Product> watermarkProductDataStream = productDataStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Product>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Product>() {
                    @Override
                    public long extractTimestamp(Product product, long recordTimestamp) {
                        return product.ingestTime;
                    }
                })
        );
        DataStream<Order> watermarkOrderDataStream = orderDataStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Order>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Order>() {
                    @Override
                    public long extractTimestamp(Order order, long recordTimestamp) {
                        return order.ingestTime;
                    }
                })
        );

        DataStream<Envelope<Order, Product>> unnestOrderProductDataStream = OneToMany.<Order, Product>create(
                new FlatMapFunction<Order, Product>() {
                    @Override
                    public void flatMap(Order order, Collector<Product> out) throws Exception {
                        order.productList.forEach(out::collect);
                    }
                }, watermarkOrderDataStream);

        DataStream<Envelope<Order, Product>> joinDataStream = unnestOrderProductDataStream
                .join(watermarkProductDataStream)
                .where(new KeySelector<Envelope<Order, Product>, String>() {
                    @Override
                    public String getKey(Envelope<Order, Product> envelope) throws Exception {
                        return envelope.getMany().id;
                    }
                })
                .equalTo(new KeySelector<Product, String>() {
                    @Override
                    public String getKey(Product product) throws Exception {
                        return product.id;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .apply(new FlatJoinFunction<Envelope<Order, Product>, Product, Envelope<Order, Product>>() {
                    @Override
                    public void join(Envelope<Order, Product> first, Product second, Collector<Envelope<Order, Product>> out) throws Exception {
                        /* Replace the product price with one from other stream */
                        first.many.price = second.price;
                        out.collect(first);
                    }
                });

        result = ManyToOne.<Order, Product, Order>create(new ManyToOne.MergeFunction<Order, Product>() {
            @Override
            public Order merge(Order order, List<Product> many) {
                order.productList = many;
                return order;
            }
        }, new IdentityKeySelector<Order>(), joinDataStream, TypeInformation.of(Order.class));
    }

    public DataStream<Order> getResult() {
        return result;
    }
}

