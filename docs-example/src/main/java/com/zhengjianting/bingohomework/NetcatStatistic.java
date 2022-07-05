package com.zhengjianting.bingohomework;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

public class NetcatStatistic {
    private static final String bootstrapServers = "10.16.1.118:9095,10.16.1.119:9095,10.16.1.120:9095";
    private static final String topic = "netcat-message";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> lines = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<String> process = lines
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new MyWindowFunction())
                .flatMap(new MyFlatMapFunction());

        process.print();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        process.sinkTo(sink);

        env.execute();
    }

    private static class MyWindowFunction extends ProcessAllWindowFunction<String, Tuple4<Date, Date, Integer, Integer>, TimeWindow> {
        @Override
        public void process(Context context, Iterable<String> iterable, Collector<Tuple4<Date, Date, Integer, Integer>> collector) throws Exception {
            int messageCnt = 0;
            int byteCnt = 0;
            for (String s : iterable) {
                messageCnt += 1;
                byteCnt += s.getBytes().length;
            }
            collector.collect(Tuple4.of(
                    new Date(context.window().getStart()),
                    new Date(context.window().getEnd()),
                    messageCnt,
                    byteCnt));
        }
    }

    private static class MyFlatMapFunction implements FlatMapFunction<Tuple4<Date, Date, Integer, Integer>, String> {
        @Override
        public void flatMap(Tuple4<Date, Date, Integer, Integer> dateDateIntegerIntegerTuple4, Collector<String> collector) throws Exception {
            String s = "window begin time: " + dateDateIntegerIntegerTuple4.f0.toString() +
                    ", window end time: " + dateDateIntegerIntegerTuple4.f1.toString() +
                    ", message count: " + dateDateIntegerIntegerTuple4.f2 +
                    ", message byte count: " + dateDateIntegerIntegerTuple4.f3;
            collector.collect(s);
        }
    }
}