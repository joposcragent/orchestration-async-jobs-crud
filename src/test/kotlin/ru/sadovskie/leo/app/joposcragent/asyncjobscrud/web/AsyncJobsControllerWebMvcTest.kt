package ru.sadovskie.leo.app.joposcragent.asyncjobscrud.web

import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.openapi.model.AsyncJobItem
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.openapi.model.AsyncJobStatus
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.service.AsyncJobRestService
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.kotlinModule
import java.time.OffsetDateTime
import java.util.UUID

class AsyncJobsControllerWebMvcTest {

	private lateinit var service: AsyncJobRestService
	private lateinit var mockMvc: org.springframework.test.web.servlet.MockMvc

	@BeforeEach
	fun setup() {
		service = mockk(relaxed = true)
		val jsonMapper = JsonMapper.builder().addModule(kotlinModule()).build()
		mockMvc = MockMvcBuilders
			.standaloneSetup(AsyncJobsController(service, jsonMapper))
			.setControllerAdvice(ApiExceptionHandler())
			.build()
	}

	@Test
	fun `get returns job`() {
		val id = UUID.randomUUID()
		val item = AsyncJobItem(
			uuid = id,
			name = "n",
			status = AsyncJobStatus.STARTED,
			startedAt = OffsetDateTime.parse("2020-01-01T00:00:00Z"),
		)
		every { service.get(id) } returns item
		mockMvc.perform(get("/async-jobs/$id"))
			.andExpect(status().isOk)
			.andExpect(jsonPath("$.uuid").value(id.toString()))
		verify(exactly = 1) { service.get(id) }
	}

	@Test
	fun `create delegates to service`() {
		val id = UUID.randomUUID()
		val body = """{"uuid":"$id","name":"x"}"""
		every { service.create(id, any()) } just runs
		mockMvc.perform(
			post("/async-jobs/$id")
				.contentType(MediaType.APPLICATION_JSON)
				.content(body),
		).andExpect(status().isOk)
		verify(exactly = 1) { service.create(id, any()) }
	}
}
