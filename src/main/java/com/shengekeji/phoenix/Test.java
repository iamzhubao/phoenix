package com.shengekeji.phoenix;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shengekeji.phoenix.pojo.Configs;
import com.shengekeji.phoenix.sourceAndSink.KafkaSink;
import com.shengekeji.phoenix.sourceAndSink.KafkaSource;
import com.shengekeji.phoenix.transformation.FlatMapFunc;
import com.shengekeji.phoenix.transformation.Process4Format;

public class Test {

	private final static Logger logger = LoggerFactory.getLogger(Test.class);

	public static void main(String[] args) throws Exception {
		// 测试环境mock
		String[] mockArgs = new String[] { "-flinkName=Format",
				"-kafkaParallelism=1 -apply=10 -topicIn=x -topicOut=x_0" };
		// args = mockArgs;

		// 读取参数
		Configs configs = new Configs(args);

		// 设置执行环境
		StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

		streamExecutionEnvironment
				// 添加kafka数据源steaming
				.addSource(KafkaSource.getKafkaConsumer(configs, 1))
				// 设置并行度
				.setParallelism(configs.kafkaParallelism)
				// 读取steaming数据并格式化展开
				.flatMap(new FlatMapFunc())
				// 对InMessage流根据index字段进行分组
				.keyBy("id")
				// 算法逻辑
				.process(new Process4Format())
				// 输出写入到kafka
				.addSink(KafkaSink.getKafkaProducer(configs)).name(configs.topicOut);
		// .print();

		// 执行
		streamExecutionEnvironment.execute(configs.flinkName);
	}

}
