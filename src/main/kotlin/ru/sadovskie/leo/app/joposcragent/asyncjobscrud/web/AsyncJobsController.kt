package ru.sadovskie.leo.app.joposcragent.asyncjobscrud.web

import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PatchMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.openapi.model.AsyncJobHierarchy
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.openapi.model.AsyncJobHierarchyRelatedList
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.openapi.model.AsyncJobItem
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.openapi.model.AsyncJobList
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.openapi.model.AsyncJobStatus
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.openapi.model.AsyncJobTerminalStatus
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.openapi.model.CreateAsyncJobItem
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.service.AsyncJobRestService
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.service.BadRequestException
import tools.jackson.databind.json.JsonMapper
import java.time.OffsetDateTime
import java.util.UUID

@RestController
class AsyncJobsController(
	private val service: AsyncJobRestService,
	private val jsonMapper: JsonMapper,
) {
	private val log = LoggerFactory.getLogger(javaClass)

	@PostMapping("/async-jobs/{jobUuid}")
	fun create(
		@PathVariable jobUuid: UUID,
		@RequestBody body: CreateAsyncJobItem,
	): ResponseEntity<Void> {
		if (log.isDebugEnabled) {
			log.debug("POST /async-jobs/{} body={}", jobUuid, jsonMapper.writeValueAsString(body))
		}
		service.create(jobUuid, body)
		if (log.isDebugEnabled) {
			log.debug("POST /async-jobs/{} -> 200 OK", jobUuid)
		}
		return ResponseEntity.ok().build()
	}

	@PatchMapping("/async-jobs/{jobUuid}")
	fun patch(
		@PathVariable jobUuid: UUID,
		@RequestBody body: String,
	): ResponseEntity<Void> {
		if (log.isDebugEnabled) {
			log.debug("PATCH /async-jobs/{} rawBody={}", jobUuid, body)
		}
		val node = jsonMapper.readTree(body)
		service.patch(jobUuid, node)
		if (log.isDebugEnabled) {
			log.debug("PATCH /async-jobs/{} -> 200 OK", jobUuid)
		}
		return ResponseEntity.ok().build()
	}

	@PostMapping("/async-jobs/{jobUuid}/finish/{terminalStatus}")
	fun finish(
		@PathVariable jobUuid: UUID,
		@PathVariable terminalStatus: String,
	): ResponseEntity<Void> {
		if (log.isDebugEnabled) {
			log.debug("POST /async-jobs/{}/finish/{}", jobUuid, terminalStatus)
		}
		val ts = runCatching { AsyncJobTerminalStatus.valueOf(terminalStatus) }.getOrElse {
			throw BadRequestException("invalid terminalStatus")
		}
		service.finish(jobUuid, ts)
		if (log.isDebugEnabled) {
			log.debug("POST /async-jobs/{}/finish/{} -> 200 OK", jobUuid, terminalStatus)
		}
		return ResponseEntity.ok().build()
	}

	@GetMapping("/async-jobs/{jobUuid}")
	fun get(@PathVariable jobUuid: UUID): AsyncJobItem {
		if (log.isDebugEnabled) {
			log.debug("GET /async-jobs/{}", jobUuid)
		}
		return service.get(jobUuid)
	}

	@PostMapping("/async-jobs/{jobUuid}/related/{entityKind}/{entityUuid}")
	fun addRelated(
		@PathVariable jobUuid: UUID,
		@PathVariable entityKind: String,
		@PathVariable entityUuid: UUID,
	): ResponseEntity<Void> {
		if (log.isDebugEnabled) {
			log.debug(
				"POST /async-jobs/{}/related/{}/{}",
				jobUuid,
				entityKind,
				entityUuid,
			)
		}
		service.addRelated(jobUuid, entityKind, entityUuid)
		if (log.isDebugEnabled) {
			log.debug("POST /async-jobs/{}/related/{}/{} -> 200 OK", jobUuid, entityKind, entityUuid)
		}
		return ResponseEntity.ok().build()
	}

	@GetMapping("/async-jobs/list")
	fun list(
		@RequestParam(required = false) parentJobUuid: UUID?,
		@RequestParam(required = false) status: AsyncJobStatus?,
		@RequestParam(required = false) startedBefore: OffsetDateTime?,
		@RequestParam(required = false) size: Int?,
		@RequestParam(required = false) page: Int?,
	): AsyncJobList {
		if (log.isDebugEnabled) {
			log.debug(
				"GET /async-jobs/list parentJobUuid={} status={} startedBefore={} size={} page={}",
				parentJobUuid,
				status,
				startedBefore,
				size,
				page,
			)
		}
		return service.list(parentJobUuid, status, startedBefore, size, page)
	}

	@GetMapping("/async-jobs/list/related/{entityUuid}")
	fun listRelated(
		@PathVariable entityUuid: UUID,
		@RequestParam(required = false) status: AsyncJobStatus?,
		@RequestParam(required = false) startedBefore: OffsetDateTime?,
		@RequestParam(required = false) size: Int?,
		@RequestParam(required = false) page: Int?,
	): AsyncJobList {
		if (log.isDebugEnabled) {
			log.debug(
				"GET /async-jobs/list/related/{} status={} startedBefore={} size={} page={}",
				entityUuid,
				status,
				startedBefore,
				size,
				page,
			)
		}
		return service.listRelated(entityUuid, status, startedBefore, size, page)
	}

	@GetMapping("/async-jobs/hierarchy/{parentJobUuid}")
	fun hierarchy(
		@PathVariable parentJobUuid: UUID,
		@RequestParam(required = false) size: Int?,
		@RequestParam(required = false) page: Int?,
	): AsyncJobHierarchy {
		if (log.isDebugEnabled) {
			log.debug("GET /async-jobs/hierarchy/{} size={} page={}", parentJobUuid, size, page)
		}
		return service.hierarchy(parentJobUuid, size, page)
	}

	@GetMapping("/async-jobs/hierarchy/related/{entityUuid}")
	fun hierarchyRelated(
		@PathVariable entityUuid: UUID,
		@RequestParam(required = false) size: Int?,
		@RequestParam(required = false) page: Int?,
	): AsyncJobHierarchyRelatedList {
		if (log.isDebugEnabled) {
			log.debug("GET /async-jobs/hierarchy/related/{} size={} page={}", entityUuid, size, page)
		}
		return service.hierarchyRelated(entityUuid, size, page)
	}
}
