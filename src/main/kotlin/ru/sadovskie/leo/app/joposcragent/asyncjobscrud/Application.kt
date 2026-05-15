package ru.sadovskie.leo.app.joposcragent.asyncjobscrud

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication
import org.springframework.kafka.annotation.EnableKafka
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.config.AppProperties

@SpringBootApplication
@EnableKafka
@EnableConfigurationProperties(AppProperties::class)
class OrchestrationAsyncJobsCrudApplication

fun main(args: Array<String>) {
	runApplication<OrchestrationAsyncJobsCrudApplication>(*args)
}
