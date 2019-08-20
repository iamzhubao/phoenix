package com.chedankeji.phoenix.util;


import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Md5加密工具
 */
public class Md5Util {
    private static final Logger LOG = LoggerFactory.getLogger(Md5Util.class.getName());

    public static String MD5(String source) throws Exception {
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] bytes = source.getBytes();
            digest.update(bytes, 0, bytes.length);
            return Hex.encodeHexString(digest.digest());
        } catch (Exception var5) {
            throw new RuntimeException("Compute Counter MD5 failed.", var5);
        }
    }

}