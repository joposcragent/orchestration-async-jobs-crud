package ru.sadovskie.leo.app.joposcragent.asyncjobscrud.web

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

	@PostMapping("/async-jobs/{jobUuid}")
	fun create(
		@PathVariable jobUuid: UUID,
		@RequestBody body: CreateAsyncJobItem,
	): ResponseEntity<Void> {
		service.create(jobUuid, body)
		return ResponseEntity.ok().build()
	}

	@PatchMapping("/async-jobs/{jobUuid}")
	fun patch(
		@PathVariable jobUuid: UUID,
		@RequestBody body: String,
	): ResponseEntity<Void> {
		val node = jsonMapper.readTree(body)
		service.patch(jobUuid, node)
		return ResponseEntity.ok().build()
	}

	@PostMapping("/async-jobs/{jobUuid}/finish/{terminalStatus}")
	fun finish(
		@PathVariable jobUuid: UUID,
		@PathVariable terminalStatus: String,
	): ResponseEntity<Void> {
		val ts = runCatching { AsyncJobTerminalStatus.valueOf(terminalStatus) }.getOrElse {
			throw BadRequestException("invalid terminalStatus")
		}
		service.finish(jobUuid, ts)
		return ResponseEntity.ok().build()
	}

	@GetMapping("/async-jobs/{jobUuid}")
	fun get(@PathVariable jobUuid: UUID): AsyncJobItem = service.get(jobUuid)

	@PostMapping("/async-jobs/{jobUuid}/related/{entityKind}/{entityUuid}")
	fun addRelated(
		@PathVariable jobUuid: UUID,
		@PathVariable entityKind: String,
		@PathVariable entityUuid: UUID,
	): ResponseEntity<Void> {
		service.addRelated(jobUuid, entityKind, entityUuid)
		return ResponseEntity.ok().build()
	}

	@GetMapping("/async-jobs/list")
	fun list(
		@RequestParam(required = false) parentJobUuid: UUID?,
		@RequestParam(required = false) status: AsyncJobStatus?,
		@RequestParam(required = false) startedBefore: OffsetDateTime?,
		@RequestParam(required = false) size: Int?,
		@RequestParam(required = false) page: Int?,
	): AsyncJobList = service.list(parentJobUuid, status, startedBefore, size, page)

	@GetMapping("/async-jobs/list/related/{entityUuid}")
	fun listRelated(
		@PathVariable entityUuid: UUID,
		@RequestParam(required = false) status: AsyncJobStatus?,
		@RequestParam(required = false) startedBefore: OffsetDateTime?,
		@RequestParam(required = false) size: Int?,
		@RequestParam(required = false) page: Int?,
	): AsyncJobList = service.listRelated(entityUuid, status, startedBefore, size, page)

	@GetMapping("/async-jobs/hierarchy/{parentJobUuid}")
	fun hierarchy(
		@PathVariable parentJobUuid: UUID,
		@RequestParam(required = false) size: Int?,
		@RequestParam(required = false) page: Int?,
	): AsyncJobHierarchy = service.hierarchy(parentJobUuid, size, page)

	@GetMapping("/async-jobs/hierarchy/related/{entityUuid}")
	fun hierarchyRelated(
		@PathVariable entityUuid: UUID,
		@RequestParam(required = false) size: Int?,
		@RequestParam(required = false) page: Int?,
	): AsyncJobHierarchyRelatedList = service.hierarchyRelated(entityUuid, size, page)
}
