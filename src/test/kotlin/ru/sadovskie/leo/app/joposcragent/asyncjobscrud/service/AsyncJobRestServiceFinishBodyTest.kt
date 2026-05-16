package ru.sadovskie.leo.app.joposcragent.asyncjobscrud.service

import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.jooq.JSON
import org.jooq.JSONB
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.config.AppProperties
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.jooq.enums.AsyncJobStatus
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.jooq.tables.records.AsyncJobsRecord
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.openapi.model.AsyncJobTerminalStatus
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.repository.AsyncJobRepository
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.kotlinModule
import java.util.UUID

class AsyncJobRestServiceFinishBodyTest {

	private val jsonMapper = JsonMapper.builder().addModule(kotlinModule()).build()
	private val repository: AsyncJobRepository = mockk(relaxed = true)
	private val mapper: AsyncJobMapper = mockk()
	private val appProperties = AppProperties().apply { autoresolveParentTasks = false }
	private lateinit var service: AsyncJobRestService

	@BeforeEach
	fun setup() {
		service = AsyncJobRestService(repository, mapper, jsonMapper, appProperties)
	}

	@Test
	fun `finish rejects unknown json field`() {
		val id = UUID.randomUUID()
		stubStartedJob(id, parentUuid = null)
		assertThrows<BadRequestException> {
			service.finish(id, AsyncJobTerminalStatus.SUCCEEDED, jsonMapper.readTree("""{"extra":1}"""))
		}
	}

	@Test
	fun `finish with empty object updates status only`() {
		val id = UUID.randomUUID()
		stubStartedJob(id, parentUuid = null)
		service.finish(id, AsyncJobTerminalStatus.SUCCEEDED, jsonMapper.readTree("{}"))
		verify(exactly = 1) {
			repository.finishJob(
				uuid = id,
				status = AsyncJobStatus.SUCCEEDED,
				resultJsonb = null,
				updateResult = false,
				contextJson = null,
				clearContext = false,
				updateContext = false,
			)
		}
	}

	@Test
	fun `finish passes result and context to repository`() {
		val id = UUID.randomUUID()
		stubStartedJob(id, parentUuid = null)
		val resultSlot = slot<JSONB>()
		val ctxSlot = slot<JSON>()
		every {
			repository.finishJob(
				id,
				AsyncJobStatus.SUCCEEDED,
				capture(resultSlot),
				true,
				capture(ctxSlot),
				false,
				true,
			)
		} answers { }
		every { repository.contextJsonFromNode(any()) } returns JSON.valueOf("""{"a":1}""")
		every { repository.resultJsonbFromNode(any()) } returns JSONB.valueOf("""{"b":2}""")
		service.finish(
			id,
			AsyncJobTerminalStatus.SUCCEEDED,
			jsonMapper.readTree("""{"context":{"a":1},"result":{"b":2}}"""),
		)
		assertTrue(resultSlot.isCaptured)
		assertTrue(ctxSlot.isCaptured)
	}

	@Test
	fun `finish with null context clears context`() {
		val id = UUID.randomUUID()
		stubStartedJob(id, parentUuid = null)
		service.finish(id, AsyncJobTerminalStatus.SUCCEEDED, jsonMapper.readTree("""{"context":null}"""))
		verify(exactly = 1) {
			repository.finishJob(
				uuid = id,
				status = AsyncJobStatus.SUCCEEDED,
				resultJsonb = null,
				updateResult = false,
				contextJson = null,
				clearContext = true,
				updateContext = true,
			)
		}
	}

	@Test
	fun `finish rejects non object context`() {
		val id = UUID.randomUUID()
		stubStartedJob(id, parentUuid = null)
		assertThrows<BadRequestException> {
			service.finish(id, AsyncJobTerminalStatus.SUCCEEDED, jsonMapper.readTree("""{"context":[]}"""))
		}
	}

	private fun stubStartedJob(id: UUID, parentUuid: UUID?) {
		val row = mockk<AsyncJobsRecord>(relaxed = true)
		every { row.status } returns AsyncJobStatus.STARTED
		every { row.parentUuid } returns parentUuid
		every { repository.findByUuid(id) } returns row
	}
}
