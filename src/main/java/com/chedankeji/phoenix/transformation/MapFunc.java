package com.chedankeji.phoenix.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chedankeji.phoenix.pojo.Message;

/**
 * 转换函数，用于将object1转换成object2
 */
public class MapFunc implements MapFunction<Message, Message> {

	private final static Logger logger = LoggerFactory.getLogger(MapFunc.class);

	private int bucketSize;

	@Override
	public Message map(Message contentMessage) throws Exception {
		Message outMessage = new Message();

		// todo something else

		return outMessage;
	}

}
