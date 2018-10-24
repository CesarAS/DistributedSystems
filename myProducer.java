

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileNotFoundException;


import org.apache.kafka.clients.producer.KafkaProducer;	
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class myProducer {

		KafkaProducer<String, String> producer;
		myProducer() {
			Properties props = new Properties();
			//Defining producer properties
			props.put("bootstrap.servers", "localhost:9092, localhost:9093");
			props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");			
			props.put("acks", "all");			
			props.put("retries", "10");			
			props.put("max.in.flight.requests.per.connection" , "3");			
			props.put("request.timeout.ms", "10000");
			props.put("batch.size", "2048");
			
			producer = new KafkaProducer<>(props);
		}
		void produce() throws InterruptedException {
			//Parameters to open file where data is stored
	        String data_file = "/home/cesar/Escritorio/ambient.csv";
	        BufferedReader buffer = null;
	        String line = "";
	        String separator = ";";
	        
	        int i = 1;
	        
	        try {
	        	//File is read line by line and data published in the corresponding topic. Send leaves Callback object waiting for confirmation
	            buffer = new BufferedReader(new FileReader(data_file));
	            while ((line = buffer.readLine()) != null) {
	            	String[] value = line.split(separator);
	                //Magnitude 1 is SO2           
	                if (value[3].equals("1")) {
	                	for (int j = 1; j<25; j++) {
	                		ProducerRecord<String, String> record = new ProducerRecord<>("SO2", Integer.toString(i),
	                				"M" + value[6] + "D" + value [7] + "H" + Integer.toString(j) + "V" + value [2*j+6]);
	                		producer.send(record, new DemoProducerCallback());
	                		i++;
	                	}
	                	//Magnitude 7 is NO   
	                } else if (value[3].equals("7")){
	                	for (int j = 1; j<25; j++) {
	                		ProducerRecord<String, String> record = new ProducerRecord<>("NO", Integer.toString(i),
	                				"M" + value[6] + "D" + value [7] + "H" + Integer.toString(j) + "V" + value [2*j+6]);
	                		producer.send(record, new DemoProducerCallback());
	                		i++;
	                	}
	                	//Magnitude 8 is NO2   
	                } else if (value[3].equals("8")) {
	                	for (int j = 1; j<25; j++) {
	                		ProducerRecord<String, String> record = new ProducerRecord<>("NO2", Integer.toString(i),
	                				"M" + value[6] + "D" + value [7] + "H" + Integer.toString(j) + "V" + value [2*j+6]);
	                		producer.send(record, new DemoProducerCallback());
	                		i++;
	                	}
	                	//Magnitude 9 is PM2.5   
	                } else if (value[3].equals("9")) {
	                	for (int j = 1; j<25; j++) {
	                		ProducerRecord<String, String> record = new ProducerRecord<>("PM2.5", Integer.toString(i),
	                				"M" + value[6] + "D" + value [7] + "H" + Integer.toString(j) + "V" + value [2*j+6]);
	                		producer.send(record, new DemoProducerCallback());
	                		i++;
	                	}
	                	//Magnitude 10 is PM10
	                } else if (value[3].equals("10")) {
	                	for (int j = 1; j<25; j++) {
	                		ProducerRecord<String, String> record = new ProducerRecord<>("PM10", Integer.toString(i),
	                				"M" + value[6] + "D" + value [7] + "H" + Integer.toString(j) + "V" + value [2*j+6]);
	                		producer.send(record, new DemoProducerCallback());
	                		i++;
	                	}
	                }
	                //Leaving random delay from 0 to 200 ms between produced messages
	                Random random = new Random();
		        	long time = random.nextInt(200);
		        	TimeUnit.MILLISECONDS.sleep(time);
	            }

	        } catch (FileNotFoundException e) {
	            e.printStackTrace();
	        } catch (IOException e) {
	            e.printStackTrace();
	        } finally {
	            if (buffer != null) {
	                try {
	                    buffer.close();
	                } catch (IOException e) {
	                    e.printStackTrace();
	                }
	            }
	        }
	        
		}
		//Stop the producer after execution
		void stop() {
			producer.close();
		}
		
		public static void main(String[] args) {
			myProducer producer = new myProducer(); //Create producer object
			try {
					producer.produce(); //Generate message
				} catch (InterruptedException e) {
					e.printStackTrace();
				} finally {
				producer.stop(); //Finally close
			}
		}
		
		//Defining DemoProducerCallback for aynchronous message generation. Implements OnCompletion method
	    private class DemoProducerCallback implements Callback {
	        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
	            if (e != null) {
	                System.out.println("Error producing to topic " + recordMetadata.topic());
	                e.printStackTrace();
	            }
	        }
	    }
}
