package com.chedankeji.phoenix.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chedankeji.phoenix.pojo.Configs;
import com.chedankeji.phoenix.sourceAndSink.SocketSource;
import com.chedankeji.phoenix.transformation.FlatMapFunc;

/**
 * 数据读取，结构化处理的例子
 */

public class FlatMapDemo {
	private final static Logger logger = LoggerFactory.getLogger(FlatMapDemo.class);

	public static void main(String[] args) throws Exception {
		// 测试环境mock
		String[] mockArgs = new String[] { "kafkaParallelism=1", "apply=test", "topicIn=test_in", "topicOut=test_out" };

		// 读取参数
		Configs configs = new Configs(mockArgs);

		// 设置执行环境
		StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

		streamExecutionEnvironment
				// 设置并行度
				.setParallelism(configs.kafkaParallelism)
				// 添加mock数据源steaming
				.addSource(new SocketSource())
				// 读取steaming数据并格式化展开
				.flatMap(new FlatMapFunc())
				// 输出到控制台
				.print();

		// 执行
		streamExecutionEnvironment.execute(configs.flinkName);

	}

}
