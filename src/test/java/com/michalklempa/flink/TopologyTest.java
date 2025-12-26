package com.michalklempa.flink;

import com.ibm.icu.text.RuleBasedNumberFormat;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;


class TopologyTest {

    @BeforeAll
    public static void init() {
        MiniClusterWithClientResource flinkCluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setNumberSlotsPerTaskManager(2)
                                .setNumberTaskManagers(1)
                                .build());
    }

    public static String numberToWords(int number) {
        RuleBasedNumberFormat formatter = new RuleBasedNumberFormat(
                Locale.US,
                RuleBasedNumberFormat.SPELLOUT
        );
        return formatter.format(number)
                .replace(" ", "")
                .replace("-", "")
                .toLowerCase();
    }

    long epoch(String datetime) {
        return Instant.parse(datetime).toEpochMilli();
    }

    List<Product> makeIdOnlyProducts(Long timestamp, Integer... ids) {
        return Arrays.stream(ids)
                .map(id -> new Product(numberToWords(id), timestamp, null))
                .collect(Collectors.toList());
    }

    List<Product> makeProducts(Long timestamp, Integer... prices) {
        return Arrays.stream(prices)
                .map(price -> new Product(numberToWords(price), timestamp, price))
                .collect(Collectors.toList());
    }

    Order makeOrder(Long timestamp, Integer... productIds) {
        return new Order("Order at " + Instant.ofEpochMilli(timestamp).toString(),
                timestamp,
                makeIdOnlyProducts(timestamp, productIds));
    }

    @Test
    void testTopology() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<Product> productList = makeProducts(epoch("2025-12-26T06:00:00Z"), 100, 200, 300, 400);

        List<Order> orderList = Arrays.asList(
            makeOrder(epoch("2025-12-26T06:00:01Z"), 100, 100, 100, 200),
            makeOrder(epoch("2025-12-26T06:00:02Z"), 200, 300),
            makeOrder(epoch("2025-12-26T06:00:03Z"), 400, 300, 200, 100),
            makeOrder(epoch("2025-12-26T06:00:04Z"), 100, 200, 300, 400)
        );

        DataStream<Order> orderDataStream = env.fromCollection(orderList);
        orderDataStream.print();
        DataStream<Product> productDataStream = env.fromCollection(productList);

        Topology topology = new Topology(orderDataStream, productDataStream);
        topology.getResult().print();
        env.execute("Test");
    }
}
