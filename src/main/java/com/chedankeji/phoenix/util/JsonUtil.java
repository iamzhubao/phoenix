package com.chedankeji.phoenix.util;

import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

public class JsonUtil {
	public static final ObjectMapper mapper = new ObjectMapper();

	public JsonUtil() {
	}

	public static String ObjectToJsonString(Object object) throws Exception {
		try {
			StringWriter e = new StringWriter();
			mapper.writeValue(e, object);
			return e.toString();
		} catch (Exception var2) {
			throw new Exception("ObjectToJsonString faile object=" + object, var2);
		}
	}

	public static <T> T JsonStringToObject(String json, Class<T> klass) throws Exception {
		try {
			return mapper.readValue(json, klass);
		} catch (Exception var3) {
			throw new Exception("JsonStringToObject failed json string is" + json, var3);
		}
	}

	public static <T> List<T> JsonStringToList(String json, Class<T> klass) throws Exception {
		try {
			return (List) mapper.readValue(json, mapper.getTypeFactory().constructCollectionType(List.class, klass));
		} catch (Exception var3) {
			throw new Exception("JsonStringToList failed json string is" + json, var3);
		}
	}

	public static <K, V> Map<K, V> JsonStringToMap(String json, Class<K> keyClass, Class<V> valueClass)
			throws Exception {
		try {
			return (Map) mapper.readValue(json,
					mapper.getTypeFactory().constructMapLikeType(Map.class, keyClass, valueClass));
		} catch (Exception var4) {
			throw new Exception("JsonStringToMap failed json string is" + json, var4);
		}
	}

	static {
		mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}
}