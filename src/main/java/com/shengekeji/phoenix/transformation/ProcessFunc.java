package com.shengekeji.phoenix.transformation;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import com.shengekeji.phoenix.pojo.Message;

/**
 * 处理函数
 */
public class ProcessFunc extends ProcessFunction<Message, Message> {

	// 中间状态
	private MapState<String, Message> tsState;

	@Override
	public void open(Configuration parameters) {
		tsState = getRuntimeContext().getMapState(new MapStateDescriptor<>("tsState", String.class, Message.class));
	}

	@Override
	public void processElement(Message message, Context context, Collector<Message> collector) throws Exception {
		String id = message.id;

		if (tsState.get(id) == null) {
			// 设置中间状态
			tsState.put(id, message);
			// todo something else
		} else {
			Message outMessage = new Message();

			outMessage.id = message.id;
			outMessage.ts = message.ts;
			outMessage.vals = message.vals;
			outMessage.p = message.p;
			// 读取缓存的值
			// 输出
			collector.collect(outMessage);
		}
	}

	@Override
	public void onTimer(long l, OnTimerContext onTimerContext, Collector<Message> collector) throws Exception {

	}

}
