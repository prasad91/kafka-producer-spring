package com.example.prasad.kafka.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ApacheKafkaWebController {

	@Value(value = "${spring.kafka.producer.topic-name}")
	private String topicName;

	@Value(value = "${spring.kafka.producer.topic-name2}")
	private String topicName2;

	@GetMapping(value = "/producer")
	public String producer(@RequestParam("message") String message) {

		sendMessage(topicName, message);

		return "Message sent to the Kafka";
	}

	@GetMapping(value = "/producer2")
	public String producer2(@RequestParam("message") String message) {

		sendMessage(topicName2, message);

		return "Message sent to the Kafka";
	}

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	public void sendMessage(String topicName, String message) {

		ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, message);

		future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

			@Override
			public void onSuccess(SendResult<String, String> result) {
				System.out.println(
						"Sent message=[" + message + "] with offset=[" + result.getRecordMetadata().offset() + "]");
			}

			@Override
			public void onFailure(Throwable ex) {
				System.out.println("Unable to send message=[" + message + "] due to : " + ex.getMessage());
			}
		});
	}

}