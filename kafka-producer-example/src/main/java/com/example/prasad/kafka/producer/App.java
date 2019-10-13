package com.example.prasad.kafka.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Hello world!
 *
 */
@SpringBootApplication
public class App {

	public static void main(String[] args) {
		System.out.println("Hello World! - Producer");
		SpringApplication.run(App.class, args);
	}

}
