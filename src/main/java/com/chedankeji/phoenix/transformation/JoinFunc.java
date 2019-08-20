package com.chedankeji.phoenix.transformation;

import org.apache.flink.api.common.functions.JoinFunction;

import com.chedankeji.phoenix.pojo.Message;

public class JoinFunc implements JoinFunction<Message, Message, Message> {

	@Override
	public Message join(Message inMessage1, Message inMessage2) throws Exception {
		Message outMessage = new Message();

		outMessage.id = (inMessage1.id);
		outMessage.ts = (inMessage1.ts);

		outMessage.vals = (inMessage1.vals + inMessage2.vals);

		return outMessage;
	}
}
