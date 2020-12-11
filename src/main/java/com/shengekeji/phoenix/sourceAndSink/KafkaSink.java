package com.shengekeji.phoenix.sourceAndSink;

import java.util.Properties;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import com.shengekeji.phoenix.constant.Constants;
import com.shengekeji.phoenix.pojo.Configs;
import com.shengekeji.phoenix.pojo.Message;
import com.shengekeji.phoenix.serialization.OutSerializationSchema;

public class KafkaSink {

	public static FlinkKafkaProducer010<Message> getKafkaProducer(Configs configs) {
		Properties sinkProperties = new Properties();
		sinkProperties.setProperty("bootstrap.servers", Constants.BOOTSTRAP_SERVERS);

		return new FlinkKafkaProducer010<>(configs.topicOut, new OutSerializationSchema(), sinkProperties);
	}

}
