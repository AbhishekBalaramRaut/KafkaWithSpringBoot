package com.learnkafka.libraryeventsproducer.producer;

import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.servlet.annotation.HandlesTypes;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.libraryeventsproducer.domain.LibraryEvent;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LibraryEventProducer {

	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;
	
	@Autowired
	ObjectMapper objectMapper;
	
	private String topic ="library-events";
	
	public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
		
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		
		ListenableFuture<SendResult<Integer, String>>  listenableFuture 
		 			= kafkaTemplate.sendDefault(key, value);
	
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(key, value, result);
				
			}

			@Override
			public void onFailure(Throwable ex) {
				handleSuccess(key, value, ex);	
			}
		});
	}
	
	public void handleSuccess(Integer key, String value, Throwable ex) {
		log.error("Error sending the message and the exception is {}",ex.getMessage());
		
		try {
			throw ex;
		} catch (Throwable e) {
			log.error("Error in onfailure {}",e.getMessage());
		}
	}
	
	public void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
		log.info("Message send successfully for the key {} and value {}, partition is {} ",key ,value,
				result.getRecordMetadata().partition());
	}
	
	public void sendLibraryEvent_approach2(LibraryEvent libraryEvent) throws JsonProcessingException {
		
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		
		ProducerRecord<Integer,String> producerRecord= buildProducerRecord(key,value);
		ListenableFuture<SendResult<Integer, String>>  listenableFuture 
		 			= kafkaTemplate.send(producerRecord);
	
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(key, value, result);
				
			}

			@Override
			public void onFailure(Throwable ex) {
				handleSuccess(key, value, ex);	
			}
		});
	}
	
	private ProducerRecord<Integer,String> buildProducerRecord(Integer key, String value) {
		List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()));
		return new ProducerRecord<>(topic, null, key, value, recordHeaders);
	}
	
	public SendResult<Integer, String> sendLibraryEventSysnchronous(LibraryEvent libraryEvent) throws JsonProcessingException {
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);
		
		SendResult<Integer, String> sendResult = null;
		
		try {
			sendResult = kafkaTemplate.sendDefault(key, value).get();
		} catch (InterruptedException | ExecutionException e) {
			log.error("InterruptedException | ExecutionException and the exception is {}",e.getMessage());
		}
	    return sendResult;
	}
}
