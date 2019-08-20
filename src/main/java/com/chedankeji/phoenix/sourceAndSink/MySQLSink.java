package com.chedankeji.phoenix.sourceAndSink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import com.chedankeji.phoenix.constant.Constants;
import com.chedankeji.phoenix.pojo.Message;

public class MySQLSink extends RichSinkFunction<Message> {

	private static final long serialVersionUID = 1L;
	private Connection connection;
	private String tableName;

	private PreparedStatement preparedStatement;

	public MySQLSink(String tableName) {
		this.tableName = tableName;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		String USERNAME = Constants.MYSQLUSER;
		String PASS = Constants.MYSQLPASSWORD;
		String DRIVERNAME = Constants.MYSQLDRIVERNAME;
		String DBURL = Constants.MYSQLDRIVERURL;
		// 加载JDBC驱动
		Class.forName(DRIVERNAME);
		// 获取数据库连接
		connection = DriverManager.getConnection(DBURL, USERNAME, PASS);

		String sql = "replace " + this.tableName + " (id, ts, vals, p) values (? ,?, ?, ?)";

		preparedStatement = connection.prepareStatement(sql);
		super.open(parameters);
	}

	@Override
	public void close() throws Exception {
		if (preparedStatement != null) {
			preparedStatement.close();
		}
		if (connection != null) {
			connection.close();
		}
		super.close();
	}

	@Override
	public void invoke(Message message, Context context) {
		try {
			preparedStatement.setString(1, message.id);
			preparedStatement.setLong(2, message.ts);
			preparedStatement.setString(3, message.vals);
			preparedStatement.setDouble(4, message.p);
			preparedStatement.executeUpdate();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
