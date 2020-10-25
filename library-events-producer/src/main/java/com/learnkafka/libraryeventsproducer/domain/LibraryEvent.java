package com.learnkafka.libraryeventsproducer.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class LibraryEvent {
	private Integer libraryEventId;
	private Book book;
	
	public Integer getLibraryEventId() {
		return libraryEventId;
	}
	public Book getBook() {
		return book;
	}

	
}
