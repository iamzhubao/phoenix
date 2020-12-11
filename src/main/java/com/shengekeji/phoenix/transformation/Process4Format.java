package com.shengekeji.phoenix.transformation;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import com.alibaba.fastjson.JSONObject;
import com.shengekeji.phoenix.pojo.Message;
import com.shengekeji.phoenix.util.HashUtil;
import com.shengekeji.phoenix.util.JsonUtil;

/**
 * 处理函数
 */
public class Process4Format extends ProcessFunction<Message, Message> {

	// 中间状态
	private MapState<String, Message> timestampState;

	@Override
	public void open(Configuration parameters) {
		timestampState = getRuntimeContext()
				.getMapState(new MapStateDescriptor<>("timestampState", String.class, Message.class));
	}

	@Override
	public void processElement(Message message, Context context, Collector<Message> collector) throws Exception {
		String id = message.id;

		if (timestampState.get(id) == null) {
			// 设置中间状态
			timestampState.put(id, message);
			// todo something else
		} else {
			// 读取中间状态
			Message inMessage = timestampState.get(id);

			Message outMessage = new Message();
			outMessage.id = (message.id);
			outMessage.ts = (message.ts);
			// 读取缓存的值
			JSONObject ob = JSONObject.parseObject(message.vals);
			Map<String, Double> valsMap = new HashMap<String, Double>();
			for (String key : ob.keySet()) {
				String valString = ob.getString(key);
				String k = null;
				Double v = null;
				try {
					k = new String() + HashUtil.hash(key);
					v = Double.parseDouble(valString);
				} catch (Exception e) {
					k = new String() + HashUtil.hash(key);
					v = new Double(1.0);
				}
				valsMap.put(k, v);
			}

			outMessage.vals = JsonUtil.ObjectToJsonString(valsMap);

			// 输出
			collector.collect(outMessage);
		}
	}

	@Override
	public void onTimer(long l, OnTimerContext onTimerContext, Collector<Message> collector) throws Exception {

	}

}
