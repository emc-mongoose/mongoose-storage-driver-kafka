package com.emc.mongoose.storage.driver.kafka;

public interface KafkaNode {

	int PORT = 9092;

	static String addr() {
		final boolean ciFlag = null != System.getenv("CI");
		if(ciFlag) {
			return "storage";
		} else {
			return "localhost";
		}
	}
}
