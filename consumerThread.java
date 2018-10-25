import java.util.Properties;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class consumerThread implements Runnable {

  private final KafkaConsumer<String, String> consumer;
  private final List<String> topic;

  public consumerThread(String brokers, String groupId, List <String> topic) {
    //The consumer properties are created for every created thread
	Properties props = new Properties();
    props.put("bootstrap.servers", brokers);
    props.put("group.id", groupId);
    props.put("enable.auto.commit", "false");
    props.put("max.poll.records", "100");
    props.put("max.poll.interval.ms","60000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("heartbeat.interval.ms", "5000");
    this.consumer = new KafkaConsumer<>(props);
    this.topic = topic;
    //Subscribe to the corresponding topics
    this.consumer.subscribe(this.topic);
  }

  @Override
  public void run() {
	  try {
		  while (true) {
			  //The consumer will poll the server and wait for new records for 100 ms
			  ConsumerRecords<String, String> records = consumer.poll(100);
			  for (ConsumerRecord<String, String> record : records) {
				  System.out.println("Data: " + record.value() + ", Topic: "
						  + record.topic() + ", Offset: " + record.offset() + ", Thread: "
						  + Thread.currentThread().getId());	
				  consumer.commitSync(); // Synchronous commit to get at-least-once semantics
			  }
		  }
	  } finally {
		  consumer.close();
	  }
  }
}