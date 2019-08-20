package com.chedankeji.phoenix.watermark;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import com.chedankeji.phoenix.pojo.Message;

/**
 * 时间窗口函数的延迟设置
 */
public class DelayWatermarks implements AssignerWithPeriodicWatermarks<Message> {

	private long currentMaxTimestamp;

	@Override
	public Watermark getCurrentWatermark() {
		return new Watermark(currentMaxTimestamp - 5000);
	}

	@Override
	public long extractTimestamp(Message message, long l) {
		long timestamp = message.ts;
		currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
		return timestamp;
	}
}
