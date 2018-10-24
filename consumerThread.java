import java.util.Properties;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class consumerThread implements Runnable {

  private final KafkaConsumer<String, String> consumer;
  private final List<String> topic;

  public consumerThread(String brokers, String groupId, List <String> topic) {
    Properties prop = createConsumerConfig(brokers, groupId);
    this.consumer = new KafkaConsumer<>(prop);
    this.topic = topic;
    this.consumer.subscribe(this.topic);
  }

  private static Properties createConsumerConfig(String brokers, String groupId) {
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
    return props;
  }

  @Override
  public void run() {
	  try {
		  while (true) {
			  ConsumerRecords<String, String> records = consumer.poll(100);
			  for (ConsumerRecord<String, String> record : records) {
				  System.out.println("Message: " + record.value() + ", Topic: "
						  + record.topic() + ", Offset: " + record.offset() + ", by ThreadID: "
						  + Thread.currentThread().getId());
				  consumer.commitSync();
			  }
		  }
	  } finally {
		  consumer.close();
	  }
  }
}