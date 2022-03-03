package org.pd.streaming.connector.kafka.examples;

import java.time.LocalTime;
import java.util.Properties;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.common.serialization.StringSerializer;

public class Main1
{
	static String TOPIC_IN = "Topic1-IN";
	static String BOOTSTRAP_SERVER = "localhost:9093";

    @SuppressWarnings("serial")
	public static void main( String[] args ) throws Exception
    {
    	Producer<String> p = new Producer<String>(BOOTSTRAP_SERVER, StringSerializer.class.getName());
//    	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 6123, "out/artifacts/flink_connector_tutorials_jar/flink-connector-tutorials.jar");
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVER);
        props.put("client.id", "flink-example1");

        FlinkKafkaConsumer<KafkaRecord> kafkaConsumer = new FlinkKafkaConsumer<>(TOPIC_IN, new MySchema(), props);
		kafkaConsumer.setStartFromLatest();
		DataStream<KafkaRecord> stream = env.addSource(kafkaConsumer);

        stream
        .timeWindowAll(Time.seconds(5))
        .reduce(new ReduceFunction<KafkaRecord>()
        {
        	KafkaRecord result = new KafkaRecord();
        	@Override
			public KafkaRecord reduce(KafkaRecord record1, KafkaRecord record2) throws Exception
			{
				System.out.println(LocalTime.now() + " -> " + record1 + "   " + record2);
				result.key = record1.key;
				result.value = record1.value + record2.value;
				return result;
			}
		})
        .print();
		new NumberGenerator(p, TOPIC_IN).start();
		System.out.println( env.getExecutionPlan() );
		env.execute();
    }
}
