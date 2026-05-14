package ru.sadovskie.leo.app.joposcragent.asyncjobscrud

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.kafka.annotation.EnableKafka

@SpringBootApplication
@EnableKafka
class OrchestrationAsyncJobsCrudApplication

fun main(args: Array<String>) {
	runApplication<OrchestrationAsyncJobsCrudApplication>(*args)
}
