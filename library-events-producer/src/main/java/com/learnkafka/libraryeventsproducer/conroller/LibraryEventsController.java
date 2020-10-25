package com.learnkafka.libraryeventsproducer.conroller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.libraryeventsproducer.domain.LibraryEvent;
import com.learnkafka.libraryeventsproducer.producer.LibraryEventProducer;

import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
public class LibraryEventsController {

	@Autowired
	LibraryEventProducer libraryEventProducer;
	
	@PostMapping("/v1/libraryevent")
	public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) {
		
		try {
			//libraryEventProducer.sendLibraryEvent(libraryEvent);
			 libraryEventProducer.sendLibraryEvent_approach2(libraryEvent);
			//SendResult<Integer, String> sendResult = libraryEventProducer.sendLibraryEventSysnchronous(libraryEvent);
			//log.info("SendResult is {}", sendResult.toString());
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		
		return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
	}
}
