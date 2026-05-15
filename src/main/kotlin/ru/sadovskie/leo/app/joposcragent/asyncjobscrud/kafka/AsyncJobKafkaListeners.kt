package ru.sadovskie.leo.app.joposcragent.asyncjobscrud.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.service.AsyncJobKafkaIngestService
import tools.jackson.databind.JsonNode
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
		log.debugKafkaInbound(record)
		val type = record.headers().lastHeader("type")?.value()?.toString(Charsets.UTF_8)
			?: record.value()?.let { parseTypeFromJson(it) }
			?: run {
				if (log.isDebugEnabled) {
					log.debug("Kafka begin: no type in headers or json, key={}", record.key())
				}
				return
			}
		if (type !in AsyncJobBeginTypes.ALL) {
			if (log.isDebugEnabled) {
				log.debug(
					"Kafka begin: ignored type={} topic={} key={}",
					type,
					record.topic(),
					record.key(),
				)
			}
			return
		}
		val root = runCatching { objectMapper.readTree(record.value()) }.getOrElse {
			log.warn("begin: invalid json: {}", it.message)
			return
		}
		val payload = root.kafkaMessagePayloadOrNull() ?: run {
			log.warn("begin: missing or invalid body for type={}", type)
			return
		}
		log.info(
			"Kafka begin: dispatching type={} topic={} key={}",
			type,
			record.topic(),
			record.key(),
		)
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
		log.debugKafkaInbound(record)
		val type = record.headers().lastHeader("type")?.value()?.toString(Charsets.UTF_8)
			?: record.value()?.let { parseTypeFromJson(it) }
			?: run {
				if (log.isDebugEnabled) {
					log.debug("Kafka result: no type in headers or json, key={}", record.key())
				}
				return
			}
		if (type !in AsyncJobResultTypes.ALL) {
			if (log.isDebugEnabled) {
				log.debug(
					"Kafka result: ignored type={} topic={} key={}",
					type,
					record.topic(),
					record.key(),
				)
			}
			return
		}
		val root = runCatching { objectMapper.readTree(record.value()) }.getOrElse {
			log.warn("result: invalid json: {}", it.message)
			return
		}
		val payload = root.kafkaMessagePayloadOrNull() ?: run {
			log.warn("result: missing or invalid body for type={}", type)
			return
		}
		log.info(
			"Kafka result: dispatching type={} topic={} key={}",
			type,
			record.topic(),
			record.key(),
		)
		ingestService.handleResult(payload)
	}

	private fun parseTypeFromJson(json: String): String? =
		runCatching {
			objectMapper.readTree(json).path("headers").path("type").asText(null)
		}.getOrNull()
}

private fun Logger.debugKafkaInbound(record: ConsumerRecord<String, String>) {
	if (!isDebugEnabled) {
		return
	}
	val headersJoined = record.headers().joinToString(prefix = "[", postfix = "]") { h ->
		"${h.key()}=${h.value().toString(Charsets.UTF_8)}"
	}
	debug(
		"Kafka inbound topic={} partition={} offset={} key={} headers={} value={}",
		record.topic(),
		record.partition(),
		record.offset(),
		record.key(),
		headersJoined,
		record.value(),
	)
}

private fun JsonNode.kafkaMessagePayloadOrNull(): JsonNode? {
	val headers = get("headers")
	val payload = get("payload")
	return when {
		headers != null && headers.isObject && payload != null && payload.isObject -> payload
		isObject -> this
		else -> null
	}
}
