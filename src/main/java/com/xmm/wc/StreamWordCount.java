package com.xmm.wc;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 1，创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2，读取文本流
        DataStreamSource<String> lineDataStream = env.socketTextStream("127.0.0.1", 7777);

        // 3，转换计算
        SingleOutputStreamOperator<Tuple2<String, Long>> worldAndOneTuple = lineDataStream.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] worlds = line.split(" ");
            for (String word : worlds) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4，分组
        KeyedStream<Tuple2<String, Long>, String> worldAndOneTupleStream = worldAndOneTuple.keyBy(data -> data.f0);
        
        // 5，求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = worldAndOneTupleStream.sum(1);
        // 6,打印
        sum.print();
        // 7，启动执行
        env.execute();

    }
}
