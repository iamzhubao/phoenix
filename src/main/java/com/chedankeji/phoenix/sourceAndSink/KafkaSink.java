package com.chedankeji.phoenix.sourceAndSink;

import java.util.Properties;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import com.chedankeji.phoenix.constant.Constants;
import com.chedankeji.phoenix.pojo.Configs;
import com.chedankeji.phoenix.pojo.Message;
import com.chedankeji.phoenix.serialization.OutSerializationSchema;

public class KafkaSink {

	public static FlinkKafkaProducer010<Message> getKafkaProducer(Configs configs) {
		Properties sinkProperties = new Properties();
		sinkProperties.setProperty("bootstrap.servers", Constants.BOOTSTRAP_SERVERS);

		return new FlinkKafkaProducer010<>(configs.topicOut, new OutSerializationSchema(), sinkProperties);
	}

}
