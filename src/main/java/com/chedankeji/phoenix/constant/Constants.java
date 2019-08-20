package com.chedankeji.phoenix.constant;

public class Constants {

	public static final String BOOTSTRAP_SERVERS = "172.17.171.6:9092,172.17.171.5:9092,172.17.171.4:9092";

	public static final long CHECKPOINT_INTERNAL = 90000L;

	public static final long MIN_CHECKPOINT_INTERNAL = 30000L;

	public static final long CHECKPOINT_TIMEOUT = 10000L;

	public static final String MYSQLDRIVERURL = "jdbc:mysql://" + "172.17.171.6:12321"
			+ "/crow?useUnicode=true&characterEncoding=UTF-8";

	public static final String MYSQLUSER = "mysql";

	public static final String MYSQLPASSWORD = "~Mysql39";

	public static final String MYSQLDRIVERNAME = "com.mysql.jdbc.Driver";

	public static final int TIME_STEP = 10000;
}
