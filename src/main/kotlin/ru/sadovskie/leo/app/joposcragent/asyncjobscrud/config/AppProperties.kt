package ru.sadovskie.leo.app.joposcragent.asyncjobscrud.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("app")
class AppProperties {
	var autoresolveParentTasks: Boolean = false
}
