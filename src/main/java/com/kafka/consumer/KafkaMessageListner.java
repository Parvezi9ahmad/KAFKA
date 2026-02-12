package com.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.kafka.dto.Customer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.TopicPartition;
@Service
public class KafkaMessageListner {
	private static final Logger log = LoggerFactory.getLogger(KafkaMessageListner.class);

//	@KafkaListener(topics = "javatechidemo10",groupId = "test-group")
//	public void consume(Customer customer) {
//		log.info("consume cosumes the message {}", customer.toString());
//	}
	
	@KafkaListener(groupId = "test-group",topicPartitions = {
	        @TopicPartition(topic = "pt-topic", partitions = {"3"})
	    })
	public void consume(String customer) {
		log.info("consume cosumes the message {}", customer.toString());
	}
	/*
	 * @KafkaListener(topics = "javatechidemo-1",groupId = "jt-group") public void
	 * consume2(String message) { log.info("consumer2 cosumes the message {}",
	 * message); }
	 * 
	 * @KafkaListener(topics = "javatechidemo-1",groupId = "jt-group") public void
	 * consume3(String message) { log.info("consumer3 cosumes the message {}",
	 * message); }
	 * 
	 * @KafkaListener(topics = "javatechidemo-1",groupId = "jt-group") public void
	 * consume4(String message) { log.info("consumer4 cosumes the message {}",
	 * message); }
	 */
}
