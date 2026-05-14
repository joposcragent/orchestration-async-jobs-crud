package ru.sadovskie.leo.app.joposcragent.asyncjobscrud.kafka

object OrchestrationKafkaTopics {
	const val COLLECTION_BATCH = "async-job.collection-batch"
	const val COLLECTION_QUERY = "async-job.collection-query"
	const val JOB_POSTING_EVALUATE = "async-job.job-posting-evaluate"
	const val JOB_POSTING_CREATE = "async-job.job-posting-create"

	val ALL = arrayOf(
		COLLECTION_BATCH,
		COLLECTION_QUERY,
		JOB_POSTING_EVALUATE,
		JOB_POSTING_CREATE,
	)
}

object AsyncJobBeginTypes {
	val ALL = setOf(
		"async-job.collection-batch-begin",
		"async-job.collection-query-begin",
		"async-job.job-posting-evaluate-begin",
		"async-job.job-posting-create-begin",
	)
}

object AsyncJobResultTypes {
	val ALL = setOf(
		"async-job.job-posting-create-result",
		"async-job.job-posting-evaluate-result",
		"async-job.collection-query-result",
		"async-job.collection-batch-result",
	)
}
