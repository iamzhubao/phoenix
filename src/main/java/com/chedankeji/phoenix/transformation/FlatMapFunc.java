package com.chedankeji.phoenix.transformation;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.chedankeji.phoenix.pojo.Message;

/**
 * 展开函数.用于将object list展开，逐条return
 */
public class FlatMapFunc extends RichFlatMapFunction<String, Message> {

	private final static Logger logger = LoggerFactory.getLogger(FlatMapFunc.class);

	@Override
	public void flatMap(String message, Collector<Message> out) throws Exception {
		try {
			JSONObject ob = JSONObject.parseObject(message);
			Message inMessage = new Message();
			inMessage.id = ob.getString("id");
			inMessage.ts = ob.getLongValue("ts");
			inMessage.vals = ob.getString("vals");
			inMessage.p = ob.getDoubleValue("p");

			// System.out.println("flatMap---->" + inMessage);
			out.collect(inMessage);
		} catch (

		Exception e)

		{
			e.printStackTrace();
			logger.error(e.getMessage());
		}

	}

}
