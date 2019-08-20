package com.chedankeji.phoenix.transformation;

import java.util.Iterator;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.util.Collector;

import com.chedankeji.phoenix.pojo.Message;

public class LeftJoinFunc implements CoGroupFunction<Message, Message, Message> {

	@Override
	public void coGroup(Iterable<Message> iterable1, Iterable<Message> iterable2, Collector<Message> collector)
			throws Exception {

		Iterator<Message> it1 = iterable1.iterator();
		Iterator<Message> it2 = iterable2.iterator();

		while (it1.hasNext()) {
			Message inMessage1 = it1.next();

			boolean hadElements = false;
			Message outMessage = new Message();
			outMessage.id = (inMessage1.id);
			outMessage.ts = (inMessage1.ts);

			while (it2.hasNext()) {
				Message inMessage2 = it1.next();

				outMessage.vals = (inMessage1.vals + inMessage2.vals);
				collector.collect(outMessage);

				hadElements = true;
			}

			if (!hadElements) {

				collector.collect(outMessage);
			}
		}
	}
}
