package com.shengekeji.phoenix.util;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeUtil {

	private static final Logger LOG = LoggerFactory.getLogger(TimeUtil.class);

	public static long getStartTime() {
		return System.currentTimeMillis();
	}

	public static String parseTime(Object longTime) throws Exception {
		String value = String.valueOf(longTime);
		value = value.replaceAll("[A-Z]+", " ").trim();
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = simpleDateFormat.parse(value);

		return String.valueOf(date.getTime() / 1000L);
	}

	public static long parseTimeMillis(Object longTime) throws Exception {
		String value = String.valueOf(longTime);
		value = value.replaceAll("[A-Z]+", " ").trim();
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = simpleDateFormat.parse(value);

		return date.getTime();
	}

}
