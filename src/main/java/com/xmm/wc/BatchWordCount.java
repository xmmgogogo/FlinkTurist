package com.xmm.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取
        DataSource<String> lineDataSource = env.readTextFile("input/word.txt");

        // 将每行数据进行分词，转换为二元组类型
        FlatMapOperator<String, Tuple2<String, Long>> worldAndOneTuple = lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] worlds = line.split(" ");
            for (String word : worlds) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 分组进行统计
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = worldAndOneTuple.groupBy(0);
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);

        // 打印
        sum.print();
    }
}
