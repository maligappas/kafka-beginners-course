package tutorial1;

import java.util.Properties;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {

	public static void main(String[] args) {

		// 3 steps for producer
		//1. create producer properties
		//2. create the producer
		//3. send data
		
		final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
		
		String bootstrapServers = "127.0.0.1:9092";
		
		Properties properties = new Properties();
		
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//Create a producer
		
		KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties);
		
		//create a producer record
		
		for(int i=0; i<10; i++) {
		
		ProducerRecord<String, String> record = new ProducerRecord<String,String>("first_topic","Hello World and Hello Sunil Kumar" + Integer.toString(i));
		
		//send data -- process is async. we may not see data immediately on the consumer
		// to see data use producer.flush or producer.close method
		
		producer.send(record, new Callback() {

			public void onCompletion(RecordMetadata recordMetadata, Exception e) {
				// TODO Auto-generated method stub
				// executes every time a record is successfully sent or an exception is thrown
				
				if (e == null) {
					logger.info("this is a log");
					logger.info("Recieved new metadata. \n" + "Topic:" + recordMetadata.partition() + "\n" +
				"Offset" + recordMetadata.offset() + "\n" + "Timestamp" + recordMetadata.timestamp());
					
				}
				else
				{
					logger.error("error while producing");
				}
			}
			
		});
		}
		
		producer.flush();
		
		producer.close();
			
		
		
	}

}
