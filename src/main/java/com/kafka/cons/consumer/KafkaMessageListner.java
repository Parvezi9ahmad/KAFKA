package com.kafka.cons.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class KafkaMessageListner {
	private static final Logger log = LoggerFactory.getLogger(KafkaMessageListner.class);

	@KafkaListener(topics = "javatechidemo-1",groupId = "jt-group")
	public void consume1(String message) {
		log.info("consumer1 cosumes the message {}", message);
	}
	
	@KafkaListener(topics = "javatechidemo-1",groupId = "jt-group")
	public void consume2(String message) {
		log.info("consumer2 cosumes the message {}", message);
	}
	@KafkaListener(topics = "javatechidemo-1",groupId = "jt-group")
	public void consume3(String message) {
		log.info("consumer3 cosumes the message {}", message);
	}
	@KafkaListener(topics = "javatechidemo-1",groupId = "jt-group")
	public void consume4(String message) {
		log.info("consumer4 cosumes the message {}", message);
	}
}
