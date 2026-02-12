package com.kafka.service;

import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.kafka.dto.Customer;

@Service
public class KafkaMessagePublisher {

	@Autowired
	private KafkaTemplate<String, Object> template;

	public void sendMessageToTopic(String message) {
		CompletableFuture<SendResult<String, Object>> future = template.send("javatechidemo-1", message);
		future.whenComplete((result, ex) -> {
			if (ex == null) {
				System.out.println(
						"SENDING MESSAGE :[" + message + "] OFFSET DATA:" + result.getRecordMetadata().offset());
			} else {
				System.out.println("Unable to send the message " + message + " due to" + ex.getMessage());
			}
		});
	}

	public void sendEventsToTopic(Customer customer) {
		try {
			CompletableFuture<SendResult<String, Object>> future = template.send("javatechidemo10", customer);
			future.whenComplete((result, ex) -> {
				if (ex == null) {
					System.out.println("SENDING MESSAGE :[" + customer.toString() + "] OFFSET DATA:"
							+ result.getRecordMetadata().offset());
				} else {
					System.out
							.println("Unable to send the message " + customer.toString() + " due to" + ex.getMessage());
				}
			});
		} catch (Exception e) {
			System.out.println("ERROR: " + e.getMessage());
		}
	}
}
