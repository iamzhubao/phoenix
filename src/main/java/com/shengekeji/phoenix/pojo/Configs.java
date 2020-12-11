package com.shengekeji.phoenix.pojo;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import lombok.Data;

/**
 * Created by huzhengke on 17/7/6.
 */

@Data
public class Configs {
	public String flinkName;

	public String topicIn;

	public String topicOut;

	public String apply;

	public String groupId;

	public int kafkaParallelism;

	public boolean hasCheckpoint;

	public Configs(String[] args) throws Exception {
		Map<String, String> map = new HashMap<>();
		for (String arg : args) {
			String[] temp = StringUtils.split(arg, "=");
			map.put(temp[0].replace("-", ""), temp[1]);
		}

		if (map.containsKey("flinkName")) {
			this.flinkName = map.get("flinkName");
		} else {
			this.flinkName = "crow";
		}

		if (map.containsKey("groupId")) {
			this.groupId = map.get("groupId");
		} else {
			this.groupId = "com.shengekeji";
		}

		if (map.containsKey("topicIn")) {
			this.topicIn = map.get("topicIn");
			this.flinkName += " -in ";
			this.flinkName += this.topicIn;
		} else {
			throw new Exception("topicIn param is null!");
		}

		if (map.containsKey("topicOut")) {
			this.topicOut = map.get("topicOut");
			this.flinkName += " -out ";
			this.flinkName += this.topicOut;

		} else {
			throw new Exception("topicOut param is null!");
		}

		if (map.containsKey("kafkaParallelism")) {
			this.kafkaParallelism = Integer.parseInt(map.get("kafkaParallelism"));
		} else {
			this.kafkaParallelism = 1;
		}

		if (map.containsKey("hasCheckpoint")) {
			this.hasCheckpoint = true;
		} else {
			this.hasCheckpoint = false;
		}

		if (map.containsKey("apply")) {
			this.apply = map.get("apply");
		} else {
			this.apply = null;
		}
	}
}
