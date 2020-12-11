package com.shengekeji.phoenix.sourceAndSink;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import com.shengekeji.phoenix.constant.Constants;
import com.shengekeji.phoenix.pojo.Configs;

public class KafkaSource {

	public static FlinkKafkaConsumer010<String> getKafkaConsumer(Configs configs, int index) {
		Properties properties = new Properties();
		properties.setProperty("group.id", configs.groupId);
		properties.setProperty("bootstrap.servers", Constants.BOOTSTRAP_SERVERS);
		List<String> topics = Arrays.asList(StringUtils.split(configs.topicIn, ",")[index - 1]);

		FlinkKafkaConsumer010<String> kafkaConsumer010 = new FlinkKafkaConsumer010<String>(topics,
				new SimpleStringSchema(), properties);
		kafkaConsumer010.setStartFromLatest();

		return kafkaConsumer010;
	}

}
