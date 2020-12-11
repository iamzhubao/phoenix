package com.shengekeji.phoenix.serialization;

import org.apache.flink.streaming.util.serialization.SerializationSchema;

import com.shengekeji.phoenix.pojo.Message;
import com.shengekeji.phoenix.util.JsonUtil;

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
