package com.bib.messager;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/")
public class MessagerProducer {
	
	private static final String TOPIC = "test";
	
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;
	 
	public void sendMessage(String msg) {
	    kafkaTemplate.send(TOPIC, msg);
	}
	
	public void sendMessageRes(String message) {
        
	    ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(TOPIC, message);
	     
	    future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
	 
	        @Override
	        public void onSuccess(SendResult<String, String> result) {
	            System.out.println("Sent message=[" + message + 
	              "] with offset=[" + result.getRecordMetadata().offset() + "]");
	        }
	        @Override
	        public void onFailure(Throwable ex) {
	            System.out.println("Unable to send message=["
	              + message + "] due to : " + ex.getMessage());
	        }
	    });
	}

	@RequestMapping(value = "/producer", method = RequestMethod.POST)
	public void addMNewUsers(@RequestBody String msg) {
		sendMessageRes(msg);
	}
	
}
