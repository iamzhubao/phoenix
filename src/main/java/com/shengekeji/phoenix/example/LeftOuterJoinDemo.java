package com.shengekeji.phoenix.example;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shengekeji.phoenix.pojo.Configs;
import com.shengekeji.phoenix.pojo.Message;
import com.shengekeji.phoenix.sourceAndSink.KafkaSource;
import com.shengekeji.phoenix.transformation.FlatMapFunc;
import com.shengekeji.phoenix.transformation.LeftJoinFunc;
import com.shengekeji.phoenix.watermark.DelayWatermarks;

/**
 * 数据Left Join的例子
 */

public class LeftOuterJoinDemo {
	private final static Logger logger = LoggerFactory.getLogger(LeftOuterJoinDemo.class);

	public static void main(String[] args) throws Exception {
		// 测试环境mock
		String[] mockArgs = new String[] { "kafkaParallelism=1", "apply=test", "topicIn=simulation,mu_0",
				"topicOut=test_out" };

		// 读取参数
		Configs configs = new Configs(mockArgs);

		// 设置执行环境
		StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Message> dataStream1 = streamExecutionEnvironment.addSource(KafkaSource.getKafkaConsumer(configs, 1))
				.flatMap(new FlatMapFunc()).assignTimestampsAndWatermarks(new DelayWatermarks());
		DataStream<Message> dataStream2 = streamExecutionEnvironment.addSource(KafkaSource.getKafkaConsumer(configs, 2))
				.flatMap(new FlatMapFunc()).assignTimestampsAndWatermarks(new DelayWatermarks());

		dataStream1.coGroup(dataStream2).where(new SelectKey()).equalTo(new SelectKey())
				.window(TumblingEventTimeWindows.of(Time.seconds(20))).apply(new LeftJoinFunc())
				// 输出到控制台
				.print();

		// 执行
		streamExecutionEnvironment.execute(configs.flinkName);

	}

	public static class SelectKey implements KeySelector<Message, String> {
		@Override
		public String getKey(Message inMessage) {
			return inMessage.id;
		}
	}

}
