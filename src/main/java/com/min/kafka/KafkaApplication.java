package com.min.kafka;

import com.min.kafka.consumer.MyConsumer;
import com.min.kafka.producer.MyProducer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaApplication {

	public static void main(String[] args)throws Exception {

		MyConsumer.generalConsumeMessageAutoCommit();

		SpringApplication.run(KafkaApplication.class, args
		);
	}
}
