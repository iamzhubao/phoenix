import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chedankeji.phoenix.pojo.Configs;
import com.chedankeji.phoenix.pojo.Message;
import com.chedankeji.phoenix.sourceAndSink.KafkaSource;
import com.chedankeji.phoenix.transformation.FlatMapFunc;
import com.chedankeji.phoenix.transformation.JoinFunc;
import com.chedankeji.phoenix.watermark.DelayWatermarks;

/**
 * 数据Inner Join的例子
 */

public class InnerJoinDemo {
	private final static Logger logger = LoggerFactory.getLogger(InnerJoinDemo.class);

	public static void main(String[] args) throws Exception {
		// 测试环境mock
		String[] mockArgs = new String[] { "kafkaParallelism=1", "apply=test", "topicIn=simulation,mu_0",
				"topicOut=test_out" };

		// 读取参数
		Configs configs = new Configs(mockArgs);

		// 设置执行环境
		StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Message> dataStream1 = streamExecutionEnvironment.addSource(KafkaSource.getKafkaConsumer(configs, 1))
				.flatMap(new FlatMapFunc()).assignTimestampsAndWatermarks(new DelayWatermarks());
		DataStream<Message> dataStream2 = streamExecutionEnvironment.addSource(KafkaSource.getKafkaConsumer(configs, 2))
				.flatMap(new FlatMapFunc()).assignTimestampsAndWatermarks(new DelayWatermarks());

		dataStream1.join(dataStream2).where(new SelectKey()).equalTo(new SelectKey())
				.window(TumblingEventTimeWindows.of(Time.seconds(20))).apply(new JoinFunc())
				// 输出到控制台
				.print();

		// 执行
		streamExecutionEnvironment.execute(configs.flinkName);

	}

	public static class SelectKey implements KeySelector<Message, String> {
		@Override
		public String getKey(Message inMessage) {
			return inMessage.id;
		}
	}

}
