package yb.yadnyesh.kafkaconsumer.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class FixedRateConsumer {
	
	private static final Logger log = LoggerFactory.getLogger(FixedRateConsumer.class);
	
	@KafkaListener(topics = "t_fixedrate")
	public void consumeMessageFromKafka(String message) {
		log.info("Consuming:  {}", message);
	}

}
