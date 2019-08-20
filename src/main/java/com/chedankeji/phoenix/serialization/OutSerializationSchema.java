package com.chedankeji.phoenix.serialization;

import org.apache.flink.streaming.util.serialization.SerializationSchema;

import com.chedankeji.phoenix.pojo.Message;
import com.chedankeji.phoenix.util.JsonUtil;

public class OutSerializationSchema implements SerializationSchema<Message> {

	@Override
	public byte[] serialize(Message outMessage) {
		String result = "";
		try {

			result = JsonUtil.ObjectToJsonString(outMessage);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result.getBytes();
	}
}
