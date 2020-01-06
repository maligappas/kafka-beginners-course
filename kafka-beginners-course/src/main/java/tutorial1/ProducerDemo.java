package tutorial1;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

	public static void main(String[] args) {

		// 3 steps for producer
		//1. create producer properties
		//2. create the producer
		//3. send data
		
		String bootstrapServers = "127.0.0.1:9092";
		
		Properties properties = new Properties();
		
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//Create a producer
		
		KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties);
		
		//create a producer record
		
		ProducerRecord<String, String> record = new ProducerRecord<String,String>("first_topic","Hello World second time");
		
		//send data -- process is async. we may not see data immediately on the consumer
		// to see data use producer.flush or producer.close method
		
		producer.send(record);
		
		producer.flush();
		
		producer.close();
		
		
		
		
		
		
		
		
		
	}

}
