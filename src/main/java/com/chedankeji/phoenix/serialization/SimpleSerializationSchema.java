
import org.apache.flink.streaming.util.serialization.SerializationSchema;

public class SimpleSerializationSchema implements SerializationSchema<String> {

	@Override
	public byte[] serialize(String object) {
		return object.getBytes();
	}
}
