package ru.sadovskie.leo.app.joposcragent.asyncjobscrud.web

import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.service.BadRequestException
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.service.ConflictException
import ru.sadovskie.leo.app.joposcragent.asyncjobscrud.service.NotFoundException

@RestControllerAdvice
class ApiExceptionHandler {

	@ExceptionHandler(NotFoundException::class)
	fun notFound(): ResponseEntity<Void> = ResponseEntity.notFound().build()

	@ExceptionHandler(ConflictException::class)
	fun conflict(): ResponseEntity<Void> = ResponseEntity.status(HttpStatus.CONFLICT).build()

	@ExceptionHandler(BadRequestException::class)
	fun badRequest(e: BadRequestException): ResponseEntity<String> =
		ResponseEntity.badRequest().body(e.message ?: "bad request")

	@ExceptionHandler(IllegalArgumentException::class)
	fun illegalArgument(e: IllegalArgumentException): ResponseEntity<String> =
		ResponseEntity.badRequest().body(e.message ?: "bad request")

	@ExceptionHandler(Exception::class)
	fun uncaught(e: Exception): ResponseEntity<String> =
		ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.stackTraceToString())
}
