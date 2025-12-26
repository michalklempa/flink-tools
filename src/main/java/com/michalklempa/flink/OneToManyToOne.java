package com.michalklempa.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OneToManyToOne {
    private static final Logger logger = LoggerFactory.getLogger(OneToManyToOne.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        logger.info("Parallelism: {}", env.getParallelism());

        DataStream<Product> productDataStream = env.fromElements();

        DataStream<Order> orderDataStream = env.fromElements();
        Topology topology = new Topology(orderDataStream, productDataStream);
        topology.getResult().print();

        env.execute("OneToManyToOne");
    }
}
