package com.michalklempa.flink;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ManyToOne implements Serializable {
    public static <ONE, MANY, KEY> SingleOutputStreamOperator<ONE> create(MergeFunction<ONE, MANY> mergeFunction, KeySelector<ONE, KEY> keySelector, DataStream<Envelope<ONE, MANY>> inputStream, TypeInformation<KEY> keyTypeInformation) {
        return inputStream.<KEY>keyBy(new KeySelector<Envelope<ONE, MANY>, KEY>() {
            @Override
            public KEY getKey(Envelope<ONE, MANY> value) throws Exception {
                return keySelector.getKey(value.getOne());
            }
        }, keyTypeInformation).window(EventTimeSessionWindows.withGap(Time.milliseconds(1L))).trigger(PurgingTrigger.of(new EnvelopeCountTrigger<>())).process(
                new ProcessWindowFunction<Envelope<ONE, MANY>, ONE, KEY, TimeWindow>() {
                    @Override
                    public void process(KEY key, ProcessWindowFunction<Envelope<ONE, MANY>, ONE, KEY, TimeWindow>.Context context, Iterable<Envelope<ONE, MANY>> elements, Collector<ONE> out) throws Exception {
                        List<MANY> manyList = new ArrayList<>();
                        ONE one = null;
                        for (Envelope<ONE, MANY> envelope : elements) {
                            manyList.add(envelope.many);
                            one = envelope.one;
                        }
                        out.collect(mergeFunction.merge(one, manyList));
                    }
                }
        );
    }

    public interface MergeFunction<ONE, MANY> extends Function {
        ONE merge(ONE one, List<MANY> many);
    }

    public static class EnvelopeCountTrigger<ONE, MANY, T extends Envelope<ONE, MANY>, W extends Window> extends Trigger<T, W> {
        private final ReducingStateDescriptor<Long> countStateDescriptor =
                new ReducingStateDescriptor<>("count", new EnvelopeCountTrigger.Sum(), LongSerializer.INSTANCE);

        @Override
        public TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) throws Exception {
            ReducingState<Long> count = ctx.getPartitionedState(countStateDescriptor);
            count.add(1L);

            Integer maxCount = element.getManyCount();
            if (count.get() >= maxCount) {
                count.clear();
                return TriggerResult.FIRE;
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(W window, TriggerContext ctx) throws Exception {
            ctx.getPartitionedState(countStateDescriptor).clear();
        }

        @Override
        public boolean canMerge() {
            return true;
        }

        @Override
        public void onMerge(W window, OnMergeContext ctx) throws Exception {
            ctx.mergePartitionedState(countStateDescriptor);
        }

        private static class Sum implements ReduceFunction<Long> {
            private static final long serialVersionUID = 3490811L;

            @Override
            public Long reduce(Long value1, Long value2) throws Exception {
                return value1 + value2;
            }
        }
    }
}

