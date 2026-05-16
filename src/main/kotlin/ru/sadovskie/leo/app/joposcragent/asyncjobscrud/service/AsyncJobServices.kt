package ru.sadovskie.leo.app.joposcragent.asyncjobscrud.service

import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import org.jooq.JSON
import org.jooq.JSONB
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.jooq.enums.AsyncJobStatus as JooqStatus
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.jooq.tables.records.AsyncJobsRecord
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.config.AppProperties
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.openapi.model.AsyncJobHierarchy
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.openapi.model.AsyncJobHierarchyRelatedList
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.openapi.model.AsyncJobItem
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.openapi.model.AsyncJobList
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.openapi.model.AsyncJobStatus
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.openapi.model.AsyncJobTerminalStatus
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.openapi.model.CreateAsyncJobItem
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.repository.AsyncJobRepository
import java.time.OffsetDateTime
import java.util.UUID

@Service
class AsyncJobKafkaIngestService(
	private val repository: AsyncJobRepository,
	private val jsonMapper: JsonMapper,
	private val appProperties: AppProperties,
) {
	private val log = LoggerFactory.getLogger(javaClass)

	@Transactional
	fun handleBegin(type: String, payload: JsonNode) {
		if (log.isDebugEnabled) {
			log.debug("Kafka begin ingest: type={} payload={}", type, jsonMapper.writeValueAsString(payload))
		}
		val jobUuid = payload.uuid("jobUuid") ?: run {
			if (log.isDebugEnabled) {
				log.debug("Kafka begin: missing jobUuid, skipping")
			}
			return
		}
		val parentUuid = payload.uuid("parentJobUuid")
		val contextNode = payload.get("context")
		val ctx = repository.contextJsonFromNode(contextNode)
		val inserted = repository.insertJobIfAbsent(
			uuid = jobUuid,
			name = type,
			parentUuid = parentUuid,
			contextJson = ctx,
		)
		if (!inserted) {
			log.warn("Kafka begin: job {} already exists, skipping", jobUuid)
			return
		}
		log.info("Kafka begin: inserted job jobUuid={} name={} parentUuid={}", jobUuid, type, parentUuid)
		val entityUuid = payload.get("entityUuid")?.takeIf { !it.isNull && it.asText().isNotBlank() }
			?.let { UUID.fromString(it.asText()) }
			?: run {
				if (log.isDebugEnabled) {
					log.debug(
						"Kafka begin: missing entityUuid after insert jobUuid={} payload={}",
						jobUuid,
						jsonMapper.writeValueAsString(payload),
					)
				}
				return
			}
		when (type) {
			"async-job.collection-query-begin" -> {
				repository.insertQueryRelation(jobUuid, entityUuid)
				log.info("Kafka begin: linked QUERY jobUuid={} entityUuid={}", jobUuid, entityUuid)
			}
			"async-job.job-posting-evaluate-begin", "async-job.job-posting-create-begin" -> {
				repository.insertPostingRelation(jobUuid, entityUuid)
				log.info("Kafka begin: linked POSTING jobUuid={} entityUuid={}", jobUuid, entityUuid)
			}
			else -> {
				log.info("Kafka begin: no entity relation for begin type={} jobUuid={}", type, jobUuid)
			}
		}
	}

	@Transactional
	fun handleResult(payload: JsonNode) {
		if (log.isDebugEnabled) {
			log.debug("Kafka result ingest: payload={}", jsonMapper.writeValueAsString(payload))
		}
		val jobUuid = payload.uuid("jobUuid") ?: run {
			if (log.isDebugEnabled) {
				log.debug("Kafka result: missing jobUuid, skipping")
			}
			return
		}
		val statusStr = payload.path("status").asText(null) ?: run {
			if (log.isDebugEnabled) {
				log.debug("Kafka result: missing status jobUuid={}, skipping", jobUuid)
			}
			return
		}
		val terminal = runCatching { JooqStatus.valueOf(statusStr) }.getOrNull()
			?: run {
				if (log.isDebugEnabled) {
					log.debug("Kafka result: unknown status={} jobUuid={}, skipping", statusStr, jobUuid)
				}
				return
			}
		if (terminal == JooqStatus.STARTED) {
			if (log.isDebugEnabled) {
				log.debug("Kafka result: non-terminal status STARTED jobUuid={}, ignoring", jobUuid)
			}
			return
		}
		val row = repository.findByUuid(jobUuid) ?: run {
			log.warn("Kafka result: job {} not found", jobUuid)
			return
		}
		if (row.status != JooqStatus.STARTED) {
			log.warn("Kafka result: job {} not in STARTED state", jobUuid)
			return
		}
		val resultJson = repository.resultJsonbFromNode(payload.get("result"))
		repository.finishJob(jobUuid, terminal, resultJson, updateResult = true)
		log.info("Kafka result: finished job jobUuid={} status={}", jobUuid, terminal)
		maybeCompleteParent(row.parentUuid, jobUuid)
	}

	private fun maybeCompleteParent(parentUuid: UUID?, finishedChildUuid: UUID) {
		if (!appProperties.autoresolveParentTasks) {
			return
		}
		if (parentUuid == null) {
			return
		}
		val parent = repository.findByUuid(parentUuid) ?: return
		if (parent.status != JooqStatus.STARTED) {
			return
		}
		if (repository.countStartedSiblings(parentUuid, finishedChildUuid) == 0) {
			log.info(
				"Kafka result: all children done, completing parent parentUuid={} lastChildUuid={}",
				parentUuid,
				finishedChildUuid,
			)
			repository.finishParentIfStarted(parentUuid)
		}
	}

	private fun JsonNode.uuid(field: String): UUID? =
		get(field)?.takeIf { !it.isNull && it.asText().isNotBlank() }?.let { UUID.fromString(it.asText()) }
}

@Service
class AsyncJobRestService(
	private val repository: AsyncJobRepository,
	private val mapper: AsyncJobMapper,
	private val jsonMapper: JsonMapper,
	private val appProperties: AppProperties,
) {
	private val log = LoggerFactory.getLogger(javaClass)

	fun create(jobUuid: UUID, body: CreateAsyncJobItem) {
		if (log.isDebugEnabled) {
			log.debug("REST create jobUuid={} body={}", jobUuid, jsonMapper.writeValueAsString(body))
		}
		require(body.uuid == jobUuid) { "uuid mismatch" }
		if (repository.exists(jobUuid)) {
			throw ConflictException()
		}
		repository.insertJob(
			uuid = jobUuid,
			name = body.name,
			parentUuid = body.parentUuid,
			status = JooqStatus.STARTED,
			contextJson = repository.contextJsonFromMap(body.context),
		)
		log.info("REST create: inserted job jobUuid={} name={}", jobUuid, body.name)
	}

	@Transactional
	fun patch(jobUuid: UUID, patch: JsonNode) {
		if (log.isDebugEnabled) {
			log.debug("REST patch jobUuid={} patch={}", jobUuid, jsonMapper.writeValueAsString(patch))
		}
		val hasAny = patch.properties().any()
		if (!hasAny) {
			throw BadRequestException("empty patch")
		}
		if (repository.findByUuid(jobUuid) == null) {
			throw NotFoundException()
		}
		val name = if (patch.has("name")) patch.get("name").asText() else null
		val applyParent = patch.has("parentUuid")
		val parentUuid = if (patch.has("parentUuid") && patch.get("parentUuid").isNull) {
			null
		} else if (patch.has("parentUuid")) {
			UUID.fromString(patch.get("parentUuid").asText())
		} else {
			null
		}
		val status = if (patch.has("status")) {
			JooqStatus.valueOf(patch.get("status").asText())
		} else {
			null
		}
		val resultB = if (patch.has("result")) {
			repository.resultJsonbFromString(patch.get("result").asText(null))
		} else {
			null
		}
		val clearContext = patch.has("context") && patch.get("context").isNull
		val contextJson = if (patch.has("context") && !patch.get("context").isNull) {
			repository.contextJsonFromNode(patch.get("context"))
		} else {
			null
		}
		val finishedAt = if (patch.has("finished_at") && !patch.get("finished_at").isNull) {
			OffsetDateTime.parse(patch.get("finished_at").asText())
		} else {
			null
		}
		if (!patch.has("name") && !applyParent && !patch.has("status") && !patch.has("result") &&
			!patch.has("context") && !patch.has("finished_at")
		) {
			throw BadRequestException("no known fields")
		}
		repository.updateJobPatch(
			uuid = jobUuid,
			name = if (patch.has("name")) name else null,
			parentUuid = parentUuid,
			applyParentUuid = applyParent,
			status = status,
			resultJsonb = resultB,
			contextJson = contextJson,
			clearContext = clearContext,
			finishedAt = finishedAt,
		)
		log.info("REST patch: updated job jobUuid={}", jobUuid)
	}

	@Transactional
	fun finish(jobUuid: UUID, terminal: AsyncJobTerminalStatus, body: JsonNode?) {
		if (log.isDebugEnabled) {
			log.debug("REST finish jobUuid={} terminal={} body={}", jobUuid, terminal, body?.let { jsonMapper.writeValueAsString(it) })
		}
		val finishExtras = parseFinishOptionalBody(body)
		val row = repository.findByUuid(jobUuid) ?: throw NotFoundException()
		if (row.status != JooqStatus.STARTED) {
			throw ConflictException()
		}
		val st = JooqStatus.valueOf(terminal.value)
		repository.finishJob(
			uuid = jobUuid,
			status = st,
			resultJsonb = finishExtras.resultJsonb,
			updateResult = finishExtras.updateResult,
			contextJson = finishExtras.contextJson,
			clearContext = finishExtras.clearContext,
			updateContext = finishExtras.updateContext,
		)
		log.info("REST finish: job jobUuid={} terminal={}", jobUuid, terminal)
		maybeCompleteParent(row.parentUuid, jobUuid)
	}

	private data class FinishExtras(
		val resultJsonb: JSONB? = null,
		val updateResult: Boolean = false,
		val contextJson: JSON? = null,
		val clearContext: Boolean = false,
		val updateContext: Boolean = false,
	)

	private fun parseFinishOptionalBody(body: JsonNode?): FinishExtras {
		if (body == null || body.isNull || body.isMissingNode) {
			return FinishExtras()
		}
		if (!body.isObject) {
			throw BadRequestException("finish body must be a JSON object")
		}
		body.properties().forEach { (name, _) ->
			if (name != "context" && name != "result") {
				throw BadRequestException("unknown field: $name")
			}
		}
		var updateResult = false
		var resultJsonb: JSONB? = null
		var updateContext = false
		var clearContext = false
		var contextJson: JSON? = null
		if (body.has("result")) {
			updateResult = true
			resultJsonb = repository.resultJsonbFromNode(body.get("result"))
		}
		if (body.has("context")) {
			updateContext = true
			val ctx = body.get("context")
			if (ctx.isNull) {
				clearContext = true
			} else {
				if (!ctx.isObject) {
					throw BadRequestException("context must be object or null")
				}
				contextJson = repository.contextJsonFromNode(ctx)
			}
		}
		return FinishExtras(
			resultJsonb = resultJsonb,
			updateResult = updateResult,
			contextJson = contextJson,
			clearContext = clearContext,
			updateContext = updateContext,
		)
	}

	private fun maybeCompleteParent(parentUuid: UUID?, finishedChildUuid: UUID) {
		if (!appProperties.autoresolveParentTasks) {
			return
		}
		if (parentUuid == null) {
			return
		}
		val parent = repository.findByUuid(parentUuid) ?: return
		if (parent.status != JooqStatus.STARTED) {
			return
		}
		if (repository.countStartedSiblings(parentUuid, finishedChildUuid) == 0) {
			log.info(
				"REST finish: all children done, completing parent parentUuid={} lastChildUuid={}",
				parentUuid,
				finishedChildUuid,
			)
			repository.finishParentIfStarted(parentUuid)
		}
	}

	fun get(jobUuid: UUID): AsyncJobItem {
		val row = repository.findByUuid(jobUuid) ?: throw NotFoundException()
		val item = mapper.toApi(row)
		if (log.isDebugEnabled) {
			log.debug("REST get jobUuid={} response={}", jobUuid, jsonMapper.writeValueAsString(item))
		}
		return item
	}

	fun addRelated(jobUuid: UUID, entityKind: String, entityUuid: UUID) {
		if (log.isDebugEnabled) {
			log.debug("REST addRelated jobUuid={} entityKind={} entityUuid={}", jobUuid, entityKind, entityUuid)
		}
		if (entityKind != "POSTING" && entityKind != "QUERY") {
			throw BadRequestException("entityKind")
		}
		if (repository.findByUuid(jobUuid) == null) {
			throw NotFoundException()
		}
		val ok = when (entityKind) {
			"POSTING" -> repository.insertPostingRelation(jobUuid, entityUuid)
			"QUERY" -> repository.insertQueryRelation(jobUuid, entityUuid)
			else -> false
		}
		if (!ok) {
			throw ConflictException()
		}
		log.info("REST addRelated: jobUuid={} entityKind={} entityUuid={}", jobUuid, entityKind, entityUuid)
	}

	fun list(
		parentJobUuid: UUID?,
		status: AsyncJobStatus?,
		startedBefore: OffsetDateTime?,
		size: Int?,
		page: Int?,
	): AsyncJobList {
		val sz = normalizeSize(size)
		val pg = normalizePage(page)
		val st = status?.let { JooqStatus.valueOf(it.value) }
		val rows = repository.listJobs(parentJobUuid, st, startedBefore, sz, pg)
		val list = AsyncJobList(list = rows.map { mapper.toApi(it) })
		log.info(
			"REST list: returned {} job(s) parentJobUuid={} status={} size={} page={}",
			list.list.size,
			parentJobUuid,
			status,
			sz,
			pg,
		)
		if (log.isDebugEnabled) {
			log.debug("REST list: response={}", jsonMapper.writeValueAsString(list))
		}
		return list
	}

	fun listRelated(
		entityUuid: UUID,
		status: AsyncJobStatus?,
		startedBefore: OffsetDateTime?,
		size: Int?,
		page: Int?,
	): AsyncJobList {
		val sz = normalizeSize(size)
		val pg = normalizePage(page)
		val st = status?.let { JooqStatus.valueOf(it.value) }
		val rows = repository.listJobsRelatedToEntity(entityUuid, st, startedBefore, sz, pg)
		val list = AsyncJobList(list = rows.map { mapper.toApi(it) })
		log.info(
			"REST listRelated: returned {} job(s) entityUuid={} status={} size={} page={}",
			list.list.size,
			entityUuid,
			status,
			sz,
			pg,
		)
		if (log.isDebugEnabled) {
			log.debug("REST listRelated: response={}", jsonMapper.writeValueAsString(list))
		}
		return list
	}

	fun hierarchy(parentJobUuid: UUID, size: Int?, page: Int?): AsyncJobHierarchy {
		val rootRow = repository.findByUuid(parentJobUuid) ?: throw NotFoundException()
		val descendants = repository.fetchDescendantsPage(parentJobUuid, size, page)
		val tree = mapper.toHierarchy(rootRow, descendants)
		log.info(
			"REST hierarchy: parentJobUuid={} descendantCount={}",
			parentJobUuid,
			descendants.size,
		)
		if (log.isDebugEnabled) {
			log.debug("REST hierarchy: response={}", jsonMapper.writeValueAsString(tree))
		}
		return tree
	}

	fun hierarchyRelated(entityUuid: UUID, size: Int?, page: Int?): AsyncJobHierarchyRelatedList {
		val sz = size?.coerceAtLeast(1) ?: Int.MAX_VALUE / 2
		val pg = normalizePage(page)
		val roots = repository.relatedRootUuidsPaged(entityUuid, sz, pg)
		if (roots.isEmpty()) {
			log.info("REST hierarchyRelated: no roots entityUuid={}", entityUuid)
			return AsyncJobHierarchyRelatedList(list = emptyList())
		}
		val list = roots.map { rootUuid ->
			val rootRow = repository.findByUuid(rootUuid)!!
			val subtree = repository.fetchSubtreeIncludingRoot(rootUuid)
			val descendants = subtree.filter { it.uuid != rootUuid }
			mapper.toHierarchy(rootRow, descendants)
		}
		val out = AsyncJobHierarchyRelatedList(list = list)
		log.info(
			"REST hierarchyRelated: entityUuid={} rootCount={}",
			entityUuid,
			list.size,
		)
		if (log.isDebugEnabled) {
			log.debug("REST hierarchyRelated: response={}", jsonMapper.writeValueAsString(out))
		}
		return out
	}

	private fun normalizeSize(size: Int?): Int = (size ?: 20).coerceAtLeast(1)

	private fun normalizePage(page: Int?): Int = (page ?: 1).coerceAtLeast(1)
}

class NotFoundException : RuntimeException()
class ConflictException : RuntimeException()
class BadRequestException(message: String) : RuntimeException(message)

@Service
class AsyncJobMapper(
	private val objectMapper: JsonMapper,
) {
	fun toApi(row: AsyncJobsRecord): AsyncJobItem {
		val ctx = row.context?.data()?.takeIf { it.isNotBlank() }?.let {
			@Suppress("UNCHECKED_CAST")
			objectMapper.readValue(it, Map::class.java) as Map<String, Any>
		}
		val resultStr = row.result?.data()?.takeIf { it.isNotBlank() }?.let { raw ->
			objectMapper.readTree(raw).toString()
		}
		return AsyncJobItem(
			uuid = row.uuid,
			name = row.name,
			status = AsyncJobStatus.valueOf(row.status.name),
			startedAt = row.startedAt,
			parentUuid = row.parentUuid,
			context = ctx,
			result = resultStr,
			updatedAt = row.updatedAt,
			finishedAt = row.finishedAt,
		)
	}

	fun toHierarchy(root: AsyncJobsRecord, descendants: List<AsyncJobsRecord>): AsyncJobHierarchy {
		val byParent = descendants.groupBy { it.parentUuid }
		fun build(node: AsyncJobsRecord): AsyncJobHierarchy {
			val children = byParent[node.uuid].orEmpty().map { build(it) }
			return AsyncJobHierarchy(root = toApi(node), children = children)
		}
		return build(root)
	}
}
