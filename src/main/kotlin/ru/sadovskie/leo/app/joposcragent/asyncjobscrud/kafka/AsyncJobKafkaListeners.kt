package ru.sadovskie.leo.app.joposcragent.asyncjobscrud.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.service.AsyncJobKafkaIngestService
import tools.jackson.databind.json.JsonMapper

@Component
class AsyncJobBeginKafkaListener(
	private val objectMapper: JsonMapper,
	private val ingestService: AsyncJobKafkaIngestService,
) {
	private val log = LoggerFactory.getLogger(javaClass)

	@KafkaListener(
		topics = [
			OrchestrationKafkaTopics.COLLECTION_BATCH,
			OrchestrationKafkaTopics.COLLECTION_QUERY,
			OrchestrationKafkaTopics.JOB_POSTING_EVALUATE,
			OrchestrationKafkaTopics.JOB_POSTING_CREATE,
		],
		groupId = "\${app.kafka.begin-consumer-group}",
	)
	fun onMessage(record: ConsumerRecord<String, String>) {
		val type = record.headers().lastHeader("type")?.value()?.toString(Charsets.UTF_8)
			?: record.value()?.let { parseTypeFromJson(it) }
			?: return
		if (type !in AsyncJobBeginTypes.ALL) {
			return
		}
		val root = runCatching { objectMapper.readTree(record.value()) }.getOrElse {
			log.warn("begin: invalid json: {}", it.message)
			return
		}
		val payload = root.get("payload") ?: run {
			log.warn("begin: missing payload for type={}", type)
			return
		}
		ingestService.handleBegin(type, payload)
	}

	private fun parseTypeFromJson(json: String): String? =
		runCatching {
			objectMapper.readTree(json).path("headers").path("type").asText(null)
		}.getOrNull()
}

@Component
class AsyncJobResultKafkaListener(
	private val objectMapper: JsonMapper,
	private val ingestService: AsyncJobKafkaIngestService,
) {
	private val log = LoggerFactory.getLogger(javaClass)

	@KafkaListener(
		topics = [
			OrchestrationKafkaTopics.COLLECTION_BATCH,
			OrchestrationKafkaTopics.COLLECTION_QUERY,
			OrchestrationKafkaTopics.JOB_POSTING_EVALUATE,
			OrchestrationKafkaTopics.JOB_POSTING_CREATE,
		],
		groupId = "\${app.kafka.result-consumer-group}",
	)
	fun onMessage(record: ConsumerRecord<String, String>) {
		val type = record.headers().lastHeader("type")?.value()?.toString(Charsets.UTF_8)
			?: record.value()?.let { parseTypeFromJson(it) }
			?: return
		if (type !in AsyncJobResultTypes.ALL) {
			return
		}
		val root = runCatching { objectMapper.readTree(record.value()) }.getOrElse {
			log.warn("result: invalid json: {}", it.message)
			return
		}
		val payload = root.get("payload") ?: run {
			log.warn("result: missing payload for type={}", type)
			return
		}
		ingestService.handleResult(payload)
	}

	private fun parseTypeFromJson(json: String): String? =
		runCatching {
			objectMapper.readTree(json).path("headers").path("type").asText(null)
		}.getOrNull()
}
