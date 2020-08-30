package yb.yadnyesh.kafkaconsumer.consumer;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import yb.yadnyesh.kafkaconsumer.entity.Employee;

@Service
public class EmployeeJsonConsumer {
	
	private static final Logger log = LoggerFactory.getLogger(EmployeeJsonConsumer.class);
	
	private ObjectMapper objectMapper = new ObjectMapper();
	
	@KafkaListener(topics = "t_employee")
	public void consumeEmployeeFromKafka(String message) throws JsonParseException, JsonMappingException, IOException {
		var emp = objectMapper.readValue(message, Employee.class);
		log.info("Employee is {}", emp);
	}
	
	
}
