import nu.studer.gradle.jooq.JooqEdition
import org.jooq.meta.jaxb.Database
import org.jooq.meta.jaxb.Generate
import org.jooq.meta.jaxb.Generator
import org.jooq.meta.jaxb.Jdbc
import org.jooq.meta.jaxb.Target
import org.openapitools.generator.gradle.plugin.tasks.GenerateTask

plugins {
	kotlin("jvm") version "2.2.21"
	kotlin("plugin.spring") version "2.2.21"
	id("org.springframework.boot") version "4.0.5"
	id("io.spring.dependency-management") version "1.1.7"
	id("nu.studer.jooq") version "9.0"
	id("org.openapi.generator") version "7.14.0"
}

group = "ru.sadovskie.leo.app.joposcragent"
version = "1.1.1"

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(21)
	}
}

repositories {
	mavenCentral()
}

val openapiOutputDir = layout.buildDirectory.dir("generated/openapi").get().asFile.path

tasks.named<GenerateTask>("openApiGenerate") {
	generatorName.set("kotlin")
	val specFile = layout.projectDirectory.file("../../specifications/services/orchestration-async-jobs-crud/openapi.yaml").asFile
	inputSpec.set(specFile.toURI().toString())
	outputDir.set(openapiOutputDir)
	packageName.set("ru.sadovskie.leo.app.joposcragent.asyncjobscrud.openapi")
	apiPackage.set("ru.sadovskie.leo.app.joposcragent.asyncjobscrud.openapi.api")
	modelPackage.set("ru.sadovskie.leo.app.joposcragent.asyncjobscrud.openapi.model")
	configOptions.set(
		mapOf(
			"serializationLibrary" to "jackson",
			"enumPropertyNaming" to "UPPERCASE",
			"useSpringBoot3" to "true",
			"documentationProvider" to "none",
		),
	)
	globalProperties.set(
		mapOf(
			"models" to "",
			"modelDocs" to "false",
			"apis" to "false",
			"supportingFiles" to "false",
		),
	)
}

dependencies {
	implementation("org.springframework.boot:spring-boot-starter-actuator")
	implementation("org.springframework.boot:spring-boot-starter-jooq")
	implementation("org.springframework.boot:spring-boot-starter-validation")
	implementation("org.springframework.boot:spring-boot-starter-webmvc")
	implementation("org.springframework.boot:spring-boot-starter-kafka")
	implementation("org.postgresql:postgresql")
	implementation("org.jetbrains.kotlin:kotlin-reflect")
	implementation("tools.jackson.module:jackson-module-kotlin")

	jooqGenerator("org.postgresql:postgresql")

	testImplementation("org.springframework.boot:spring-boot-starter-actuator-test")
	testImplementation("org.springframework.boot:spring-boot-starter-jooq-test")
	testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testImplementation("org.springframework.boot:spring-boot-testcontainers")
	testImplementation("org.springframework.kafka:spring-kafka-test")
	testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
	testImplementation("org.testcontainers:testcontainers-junit-jupiter")
	testImplementation("org.testcontainers:postgresql:1.20.4")
	testImplementation("org.testcontainers:kafka:1.20.4")
	testImplementation("io.mockk:mockk-jvm:1.14.6")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

kotlin {
	compilerOptions {
		freeCompilerArgs.addAll("-Xjsr305=strict", "-Xannotation-default-target=param-property")
	}
}

sourceSets["main"].kotlin.srcDir("$openapiOutputDir/src/main/kotlin")

val jooqDbUrl = System.getenv("JOOQ_DB_URL") ?: "jdbc:postgresql://localhost:5432/joposcragent"
val jooqDbUser = System.getenv("JOOQ_DB_USER") ?: "postgres"
val jooqDbPassword = System.getenv("JOOQ_DB_PASSWORD") ?: "postgres"

jooq {
	version.set("3.19.18")
	edition.set(JooqEdition.OSS)
	configurations {
		create("main") {
			generateSchemaSourceOnCompilation.set(false)
			jooqConfiguration.apply {
				jdbc = Jdbc().apply {
					driver = "org.postgresql.Driver"
					url = jooqDbUrl
					user = jooqDbUser
					password = jooqDbPassword
				}
				generator = Generator().apply {
					database = Database().apply {
						name = "org.jooq.meta.postgres.PostgresDatabase"
						inputSchema = "orchestration"
						excludes = "flyway_schema_history"
					}
					generate = Generate().apply {
						isDeprecated = false
						isRecords = true
						isImmutablePojos = true
						isFluentSetters = true
					}
					target = Target().apply {
						packageName = "ru.sadovskie.leo.app.joposcragent.asyncjobscrud.jooq"
						directory = "build/generated-src/jooq/main"
					}
				}
			}
		}
	}
}

sourceSets["main"].java.srcDir("build/generated-src/jooq/main")

tasks.named("compileKotlin") {
	dependsOn("openApiGenerate", "generateJooq")
}

val dockerImageRepository = System.getenv("IMAGE_NAME") ?: "joposcragent/${rootProject.name}"
val dockerImageTag = System.getenv("IMAGE_TAG") ?: project.version.toString()

tasks.bootBuildImage {
	imageName.set("$dockerImageRepository:$dockerImageTag")
	finalizedBy("bootBuildImageTagLatest")
}

tasks.register<Exec>("bootBuildImageTagLatest") {
	group = "container"
	description = "docker tag: помечает образ из bootBuildImage тегом latest"
	commandLine(
		"docker", "tag",
		"$dockerImageRepository:$dockerImageTag",
		"$dockerImageRepository:latest",
	)
}

tasks.register("buildImage") {
	group = "container"
	description = "Build OCI image via Buildpacks (bootBuildImage)."
	dependsOn("bootBuildImage")
}

tasks.withType<Test> {
	useJUnitPlatform()
}
