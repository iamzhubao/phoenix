package com.shengekeji.phoenix.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shengekeji.phoenix.pojo.Configs;
import com.shengekeji.phoenix.sourceAndSink.SocketSource;
import com.shengekeji.phoenix.transformation.FlatMapFunc;
import com.shengekeji.phoenix.transformation.SlideWindowFunc;

/**
 * 数据读取，结构化处理，并且做滑动窗口聚合处理的例子
 *
 * print：窗口数据相加
 */

public class SlideWindowDemo {
	private final static Logger logger = LoggerFactory.getLogger(SlideWindowDemo.class);

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
				// 对InMessage流根据index字段进行分组
				.keyBy("index")
				// 滑动窗口，数据流中的数据每2个为一个窗口，每次向前滑动1个step
				.countWindow(2, 1).apply(new SlideWindowFunc())
				// 输出到控制台
				.print();

		// 执行
		streamExecutionEnvironment.execute(configs.flinkName);

	}

}
