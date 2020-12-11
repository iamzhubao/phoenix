package com.shengekeji.phoenix.sourceAndSink;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.alibaba.fastjson.JSON;
import com.shengekeji.phoenix.pojo.Message;

public class SocketSource implements SourceFunction<String> {

	private volatile boolean running = true;

	@Override
	public void run(SourceContext<String> sourceContext) throws Exception {

		while (running) {
			try {
				List<Message> inMessages = new ArrayList<>();
				for (int i = 0; i < 1; i++) {
					Message inMessage = new Message();
					inMessage.id = ("id:" + i);
					inMessage.ts = (System.currentTimeMillis() / 10000 * 10000);
					inMessages.add(inMessage);
				}
				sourceContext.collect(JSON.toJSONString(inMessages));
			} catch (Exception e) {
				e.printStackTrace();
			}
			Thread.sleep(10000);
		}

	}

	@Override
	public void cancel() {

		running = false;
	}

}
