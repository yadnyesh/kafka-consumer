package yb.yadnyesh.kafkaconsumer.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaKeyConsumer {
	
	private static final Logger log = LoggerFactory.getLogger(KafkaKeyConsumer.class);
	
	@KafkaListener(topics = "t_multipartitions", concurrency = "2")
	public void consumeKafkaMessagesWithKey(ConsumerRecord<String, String> message ) throws InterruptedException {
		log.info("Key: {}, Partition {}, Message {}", message.key(), message.partition(), message.value());
		Thread.sleep(1000);
	}

}
