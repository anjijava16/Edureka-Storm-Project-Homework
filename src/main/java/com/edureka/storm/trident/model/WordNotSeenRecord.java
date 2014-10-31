package com.edureka.storm.trident.model;

public class WordNotSeenRecord {
	

	private String word;
	private Long time;
	
	public WordNotSeenRecord(String word, Long time){
		this.word = word;
		this.time = time;
	}
	public String getWord() {
		return word;
	}
	public void setWord(String word) {
		this.word = word;
	}
	public Long getTime() {
		return time;
	}
	public void setTime(Long time) {
		this.time = time;
	}
}
