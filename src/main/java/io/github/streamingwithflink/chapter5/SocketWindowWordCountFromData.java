package io.github.streamingwithflink.chapter5;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class SocketWindowWordCountFromData {

    public static void main(String[] args) throws Exception {

        // 创建 execution environment
        String path = "/Users/xmly/IdeaProjects/examples-java/target/examples-java-1.0.jar";
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("47.116.45.30", 8081, path);

        DataStream<String> text = env.fromData(WordCountData.WORDS);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // 解析数据，按 word 分组，开窗，聚合
        DataStream<Tuple2<String, Integer>> windowCounts = text
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                        for (String word : value.split("\\s")) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        // 将结果打印到控制台，注意这里使用的是单线程打印，而非多线程
        windowCounts.print().setParallelism(1);

        env.execute("Socket Window WordCount from data");
    }
}