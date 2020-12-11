package com.shengekeji.phoenix.transformation;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shengekeji.phoenix.pojo.Message;

/**
 * 滑动窗口函数
 */
public class SlideWindowFunc extends RichWindowFunction<Message, Message, Tuple, GlobalWindow> {

	private final static Logger logger = LoggerFactory.getLogger(SlideWindowFunc.class);

	@Override
	public void apply(Tuple tuple, GlobalWindow globalWindow, Iterable<Message> iterable, Collector<Message> collector)
			throws Exception {
		Message outMessage = new Message();
		outMessage.id = (tuple.toString());

		List<Message> inMessages = new ArrayList<>();
		for (Iterator iter = iterable.iterator(); iter.hasNext();) {
			inMessages.add((Message) iter.next());
		}
		outMessage.ts = (inMessages.get(0).ts);

		collector.collect(outMessage);
	}

}
