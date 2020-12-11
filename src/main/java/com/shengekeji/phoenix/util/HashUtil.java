package com.shengekeji.phoenix.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Md5加密工具
 */
public class HashUtil {
	private static final Logger LOG = LoggerFactory.getLogger(HashUtil.class.getName());

	public static int hash(String key) throws Exception {
		int hash, i;
		for (hash = key.length(), i = 0; i < key.length(); i++)
			hash += key.charAt(i);
		return (hash % 1000 + 1);
	}

}