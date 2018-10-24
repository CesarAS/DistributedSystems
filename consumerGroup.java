import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

public final class consumerGroup {
  private final int numberOfConsumers;
  private final String groupId;
  private final List <String> topic;
  private final String brokers;
  private List<consumerThread> consumers;

  public consumerGroup(String brokers, String groupId, List <String> topic,
      int numberOfConsumers) {
    this.brokers = brokers;
    this.topic = topic;
    this.groupId = groupId;
    this.numberOfConsumers = numberOfConsumers;
    consumers = new ArrayList<>();
    for (int i = 0; i < this.numberOfConsumers; i++) {
      consumerThread ncThread =
          new consumerThread(this.brokers, this.groupId, this.topic);
      consumers.add(ncThread);
    }
  }

  public void execute() {
    for (consumerThread ncThread : consumers) {
      Thread t = new Thread(ncThread);
      t.start();
    }
  }
  
  public static void main(String[] args) {
	  
	  //PropertyConfigurator.configure();

    String brokers = "localhost:9092, localhost:9093";
    String groupId_Rain = "group-1";
    String groupId_Particles = "group-2";
    List <String> topics_Rain = Arrays.asList("SO2","NO","NO2");
    List <String> topics_Particles = Arrays.asList("PM2.5","PM10");
    int numberOfConsumer = 2;

    // Start group of Notification Consumers
    consumerGroup consumerGroup =
        new consumerGroup(brokers, groupId_Rain, topics_Rain, numberOfConsumer);
    consumerGroup consumerGroup2 =
        new consumerGroup(brokers, groupId_Particles, topics_Particles, numberOfConsumer);
    consumerGroup.execute();
    consumerGroup2.execute();

    try {
      Thread.sleep(100000);
    } catch (InterruptedException ie) {

    }
  }
}