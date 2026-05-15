package ru.sadovskie.leo.app.joposcragent.asyncjobscrud.repository

import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import org.jooq.Condition
import org.jooq.DSLContext
import org.jooq.JSON
import org.jooq.JSONB
import org.jooq.impl.DSL
import org.springframework.stereotype.Repository
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.jooq.enums.AsyncJobStatus
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.jooq.tables.AsyncJobs
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.jooq.tables.AsyncJobsToJobPostings
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.jooq.tables.AsyncJobsToSearchQueries
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.jooq.tables.records.AsyncJobsRecord
import java.time.OffsetDateTime
import java.util.UUID

@Repository
class AsyncJobRepository(
	private val dsl: DSLContext,
	private val objectMapper: JsonMapper,
) {

	fun exists(uuid: UUID): Boolean =
		dsl.fetchExists(dsl.selectFrom(AsyncJobs.ASYNC_JOBS).where(AsyncJobs.ASYNC_JOBS.UUID.eq(uuid)))

	fun findByUuid(uuid: UUID): AsyncJobsRecord? =
		dsl.selectFrom(AsyncJobs.ASYNC_JOBS).where(AsyncJobs.ASYNC_JOBS.UUID.eq(uuid)).fetchOne()

	fun insertJob(
		uuid: UUID,
		name: String,
		parentUuid: UUID?,
		status: AsyncJobStatus,
		contextJson: JSON?,
	) {
		dsl.insertInto(AsyncJobs.ASYNC_JOBS)
			.columns(
				AsyncJobs.ASYNC_JOBS.UUID,
				AsyncJobs.ASYNC_JOBS.NAME,
				AsyncJobs.ASYNC_JOBS.PARENT_UUID,
				AsyncJobs.ASYNC_JOBS.STATUS,
				AsyncJobs.ASYNC_JOBS.CONTEXT,
				AsyncJobs.ASYNC_JOBS.STARTED_AT,
			)
			.values(
				uuid,
				name,
				parentUuid,
				status,
				contextJson,
				OffsetDateTime.now(),
			)
			.execute()
	}

	/** @return true if inserted, false if conflict */
	fun insertJobIfAbsent(uuid: UUID, name: String, parentUuid: UUID?, contextJson: JSON?): Boolean {
		val inserted = dsl.insertInto(AsyncJobs.ASYNC_JOBS)
			.columns(
				AsyncJobs.ASYNC_JOBS.UUID,
				AsyncJobs.ASYNC_JOBS.NAME,
				AsyncJobs.ASYNC_JOBS.PARENT_UUID,
				AsyncJobs.ASYNC_JOBS.STATUS,
				AsyncJobs.ASYNC_JOBS.CONTEXT,
				AsyncJobs.ASYNC_JOBS.STARTED_AT,
			)
			.values(
				uuid,
				name,
				parentUuid,
				AsyncJobStatus.STARTED,
				contextJson,
				OffsetDateTime.now(),
			)
			.onConflict(AsyncJobs.ASYNC_JOBS.UUID)
			.doNothing()
			.returning(AsyncJobs.ASYNC_JOBS.UUID)
			.fetchOne()
		return inserted != null
	}

	fun updateJobPatch(
		uuid: UUID,
		name: String?,
		parentUuid: UUID?,
		applyParentUuid: Boolean,
		status: AsyncJobStatus?,
		resultJsonb: JSONB?,
		contextJson: JSON?,
		clearContext: Boolean,
		finishedAt: OffsetDateTime?,
	) {
		var step = dsl.update(AsyncJobs.ASYNC_JOBS).set(AsyncJobs.ASYNC_JOBS.UPDATED_AT, OffsetDateTime.now())
		if (name != null) {
			step = step.set(AsyncJobs.ASYNC_JOBS.NAME, name)
		}
		if (applyParentUuid) {
			step = step.set(AsyncJobs.ASYNC_JOBS.PARENT_UUID, parentUuid)
		}
		if (status != null) {
			step = step.set(AsyncJobs.ASYNC_JOBS.STATUS, status)
		}
		if (resultJsonb !== null) {
			step = step.set(AsyncJobs.ASYNC_JOBS.RESULT, resultJsonb)
		}
		if (clearContext) {
			step = step.set(AsyncJobs.ASYNC_JOBS.CONTEXT, null as JSON?)
		} else if (contextJson !== null) {
			step = step.set(AsyncJobs.ASYNC_JOBS.CONTEXT, contextJson)
		}
		if (finishedAt != null) {
			step = step.set(AsyncJobs.ASYNC_JOBS.FINISHED_AT, finishedAt)
		}
		step.where(AsyncJobs.ASYNC_JOBS.UUID.eq(uuid)).execute()
	}

	fun finishJob(uuid: UUID, status: AsyncJobStatus, resultJsonb: JSONB?, updateResult: Boolean) {
		val now = OffsetDateTime.now()
		var step = dsl.update(AsyncJobs.ASYNC_JOBS)
			.set(AsyncJobs.ASYNC_JOBS.STATUS, status)
			.set(AsyncJobs.ASYNC_JOBS.FINISHED_AT, now)
			.set(AsyncJobs.ASYNC_JOBS.UPDATED_AT, now)
		if (updateResult) {
			step = step.set(AsyncJobs.ASYNC_JOBS.RESULT, resultJsonb)
		}
		step.where(AsyncJobs.ASYNC_JOBS.UUID.eq(uuid)).execute()
	}

	fun countStartedSiblings(parentUuid: UUID, excludeChildUuid: UUID): Int =
		dsl.selectCount()
			.from(AsyncJobs.ASYNC_JOBS)
			.where(AsyncJobs.ASYNC_JOBS.PARENT_UUID.eq(parentUuid))
			.and(AsyncJobs.ASYNC_JOBS.STATUS.eq(AsyncJobStatus.STARTED))
			.and(AsyncJobs.ASYNC_JOBS.UUID.ne(excludeChildUuid))
			.fetchOne()!!.value1()

	fun finishParentIfStarted(parentUuid: UUID) {
		val now = OffsetDateTime.now()
		dsl.update(AsyncJobs.ASYNC_JOBS)
			.set(AsyncJobs.ASYNC_JOBS.STATUS, AsyncJobStatus.SUCCEEDED)
			.set(AsyncJobs.ASYNC_JOBS.FINISHED_AT, now)
			.set(AsyncJobs.ASYNC_JOBS.UPDATED_AT, now)
			.where(AsyncJobs.ASYNC_JOBS.UUID.eq(parentUuid))
			.and(AsyncJobs.ASYNC_JOBS.STATUS.eq(AsyncJobStatus.STARTED))
			.execute()
	}

	fun insertPostingRelation(jobUuid: UUID, postingUuid: UUID): Boolean {
		val inserted = dsl.insertInto(AsyncJobsToJobPostings.ASYNC_JOBS_TO_JOB_POSTINGS)
			.columns(
				AsyncJobsToJobPostings.ASYNC_JOBS_TO_JOB_POSTINGS.ASYNC_JOB_UUID,
				AsyncJobsToJobPostings.ASYNC_JOBS_TO_JOB_POSTINGS.JOB_POSTINGS_UUID,
			)
			.values(jobUuid, postingUuid)
			.onConflict(
				AsyncJobsToJobPostings.ASYNC_JOBS_TO_JOB_POSTINGS.JOB_POSTINGS_UUID,
				AsyncJobsToJobPostings.ASYNC_JOBS_TO_JOB_POSTINGS.ASYNC_JOB_UUID,
			)
			.doNothing()
			.returning(AsyncJobsToJobPostings.ASYNC_JOBS_TO_JOB_POSTINGS.UUID)
			.fetchOne()
		return inserted != null
	}

	fun insertQueryRelation(jobUuid: UUID, queryUuid: UUID): Boolean {
		val inserted = dsl.insertInto(AsyncJobsToSearchQueries.ASYNC_JOBS_TO_SEARCH_QUERIES)
			.columns(
				AsyncJobsToSearchQueries.ASYNC_JOBS_TO_SEARCH_QUERIES.ASYNC_JOB_UUID,
				AsyncJobsToSearchQueries.ASYNC_JOBS_TO_SEARCH_QUERIES.SEARCH_QUERY_UUID,
			)
			.values(jobUuid, queryUuid)
			.onConflict(
				AsyncJobsToSearchQueries.ASYNC_JOBS_TO_SEARCH_QUERIES.SEARCH_QUERY_UUID,
				AsyncJobsToSearchQueries.ASYNC_JOBS_TO_SEARCH_QUERIES.ASYNC_JOB_UUID,
			)
			.doNothing()
			.returning(AsyncJobsToSearchQueries.ASYNC_JOBS_TO_SEARCH_QUERIES.UUID)
			.fetchOne()
		return inserted != null
	}

	fun listJobs(
		parentUuid: UUID?,
		status: AsyncJobStatus?,
		startedBefore: OffsetDateTime?,
		size: Int,
		page: Int,
	): List<AsyncJobsRecord> {
		var cond: Condition = DSL.trueCondition()
		if (parentUuid != null) {
			cond = cond.and(AsyncJobs.ASYNC_JOBS.PARENT_UUID.eq(parentUuid))
		}
		if (status != null) {
			cond = cond.and(AsyncJobs.ASYNC_JOBS.STATUS.eq(status))
		}
		if (startedBefore != null) {
			cond = cond.and(AsyncJobs.ASYNC_JOBS.STARTED_AT.le(startedBefore))
		}
		val offset = (page - 1) * size
		return dsl.selectFrom(AsyncJobs.ASYNC_JOBS)
			.where(cond)
			.orderBy(AsyncJobs.ASYNC_JOBS.STARTED_AT.desc(), AsyncJobs.ASYNC_JOBS.UUID)
			.limit(size)
			.offset(offset)
			.fetch()
	}

	fun listJobsRelatedToEntity(
		entityUuid: UUID,
		status: AsyncJobStatus?,
		startedBefore: OffsetDateTime?,
		size: Int,
		page: Int,
	): List<AsyncJobsRecord> {
		val postingIds = DSL.select(AsyncJobsToJobPostings.ASYNC_JOBS_TO_JOB_POSTINGS.ASYNC_JOB_UUID)
			.from(AsyncJobsToJobPostings.ASYNC_JOBS_TO_JOB_POSTINGS)
			.where(AsyncJobsToJobPostings.ASYNC_JOBS_TO_JOB_POSTINGS.JOB_POSTINGS_UUID.eq(entityUuid))
		val queryIds = DSL.select(AsyncJobsToSearchQueries.ASYNC_JOBS_TO_SEARCH_QUERIES.ASYNC_JOB_UUID)
			.from(AsyncJobsToSearchQueries.ASYNC_JOBS_TO_SEARCH_QUERIES)
			.where(AsyncJobsToSearchQueries.ASYNC_JOBS_TO_SEARCH_QUERIES.SEARCH_QUERY_UUID.eq(entityUuid))
		val union = postingIds.union(queryIds)
		var cond: Condition = AsyncJobs.ASYNC_JOBS.UUID.`in`(union)
		if (status != null) {
			cond = cond.and(AsyncJobs.ASYNC_JOBS.STATUS.eq(status))
		}
		if (startedBefore != null) {
			cond = cond.and(AsyncJobs.ASYNC_JOBS.STARTED_AT.le(startedBefore))
		}
		val offset = (page - 1) * size
		return dsl.selectFrom(AsyncJobs.ASYNC_JOBS)
			.where(cond)
			.orderBy(AsyncJobs.ASYNC_JOBS.STARTED_AT.desc(), AsyncJobs.ASYNC_JOBS.UUID)
			.limit(size)
			.offset(offset)
			.fetch()
	}

	fun fetchDescendantsPage(parentUuid: UUID, size: Int?, page: Int?): List<AsyncJobsRecord> {
		val cteName = DSL.name("d")
		val anc = AsyncJobs.ASYNC_JOBS.`as`("anc")
		val aj = AsyncJobs.ASYNC_JOBS.`as`("aj")
		val anchor = dsl.select(anc.asterisk())
			.from(anc)
			.where(anc.PARENT_UUID.eq(parentUuid))
		val uuidFromCte = DSL.field(
			DSL.name("d", AsyncJobs.ASYNC_JOBS.UUID.name),
			AsyncJobs.ASYNC_JOBS.UUID.dataType,
		)
		val recursive = dsl.select(aj.asterisk())
			.from(aj)
			.innerJoin(DSL.table(cteName))
			.on(aj.PARENT_UUID.eq(uuidFromCte))
		val cte = cteName.`as`(anchor.unionAll(recursive))
		val ordered = dsl.withRecursive(cte)
			.selectFrom(cte)
			.orderBy(
				cte.field(AsyncJobs.ASYNC_JOBS.STARTED_AT)!!.asc().nullsLast(),
				cte.field(AsyncJobs.ASYNC_JOBS.UUID)!!.asc(),
			)
		return if (size != null && page != null) {
			val offset = (page - 1) * size
			ordered.limit(size).offset(offset).fetchInto(AsyncJobs.ASYNC_JOBS)
		} else {
			ordered.fetchInto(AsyncJobs.ASYNC_JOBS)
		}
	}

	fun fetchSubtreeIncludingRoot(rootUuid: UUID): List<AsyncJobsRecord> {
		val cteName = DSL.name("t")
		val anc = AsyncJobs.ASYNC_JOBS.`as`("anc")
		val aj = AsyncJobs.ASYNC_JOBS.`as`("aj")
		val anchor = dsl.select(anc.asterisk())
			.from(anc)
			.where(anc.UUID.eq(rootUuid))
		val uuidFromCte = DSL.field(
			DSL.name("t", AsyncJobs.ASYNC_JOBS.UUID.name),
			AsyncJobs.ASYNC_JOBS.UUID.dataType,
		)
		val recursive = dsl.select(aj.asterisk())
			.from(aj)
			.innerJoin(DSL.table(cteName))
			.on(aj.PARENT_UUID.eq(uuidFromCte))
		val cte = cteName.`as`(anchor.unionAll(recursive))
		return dsl.withRecursive(cte)
			.selectFrom(cte)
			.orderBy(
				cte.field(AsyncJobs.ASYNC_JOBS.STARTED_AT)!!.asc().nullsLast(),
				cte.field(AsyncJobs.ASYNC_JOBS.UUID)!!.asc(),
			)
			.fetchInto(AsyncJobs.ASYNC_JOBS)
	}

	fun relatedRootUuidsPaged(entityUuid: UUID, size: Int, page: Int): List<UUID> {
		val postingIds = DSL.select(AsyncJobsToJobPostings.ASYNC_JOBS_TO_JOB_POSTINGS.ASYNC_JOB_UUID)
			.from(AsyncJobsToJobPostings.ASYNC_JOBS_TO_JOB_POSTINGS)
			.where(AsyncJobsToJobPostings.ASYNC_JOBS_TO_JOB_POSTINGS.JOB_POSTINGS_UUID.eq(entityUuid))
		val queryIds = DSL.select(AsyncJobsToSearchQueries.ASYNC_JOBS_TO_SEARCH_QUERIES.ASYNC_JOB_UUID)
			.from(AsyncJobsToSearchQueries.ASYNC_JOBS_TO_SEARCH_QUERIES)
			.where(AsyncJobsToSearchQueries.ASYNC_JOBS_TO_SEARCH_QUERIES.SEARCH_QUERY_UUID.eq(entityUuid))
		val union = postingIds.union(queryIds)
		val offset = (page - 1) * size
		return dsl.select(AsyncJobs.ASYNC_JOBS.UUID)
			.from(AsyncJobs.ASYNC_JOBS)
			.where(AsyncJobs.ASYNC_JOBS.UUID.`in`(union))
			.orderBy(AsyncJobs.ASYNC_JOBS.STARTED_AT.desc(), AsyncJobs.ASYNC_JOBS.UUID)
			.limit(size)
			.offset(offset)
			.fetch(AsyncJobs.ASYNC_JOBS.UUID)
	}

	fun contextJsonFromMap(map: Map<String, Any>?): JSON? =
		map?.let { JSON.valueOf(objectMapper.writeValueAsString(it)) }

	fun contextJsonFromNode(node: JsonNode?): JSON? =
		when {
			node == null || node.isNull -> null
			node.isObject -> JSON.valueOf(objectMapper.writeValueAsString(node))
			else -> null
		}

	fun resultJsonbFromNode(node: JsonNode?): JSONB =
		when {
			node == null || node.isNull -> JSONB.valueOf("null")
			else -> JSONB.valueOf(objectMapper.writeValueAsString(node))
		}

	fun resultJsonbFromString(s: String?): JSONB? =
		s?.let { JSONB.valueOf(objectMapper.readTree(it).toString()) }
}
