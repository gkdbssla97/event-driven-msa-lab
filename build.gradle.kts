import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.tasks.testing.Test

plugins {
    id("org.springframework.boot") version "3.4.5" apply false
    id("io.spring.dependency-management") version "1.1.7" apply false
}

group = "com.example.kafkatoy"
version = "0.0.1-SNAPSHOT"

allprojects {
    repositories {
        mavenCentral()
    }
}

subprojects {
    group = rootProject.group
    version = rootProject.version

    apply(plugin = "java")

    extensions.configure<JavaPluginExtension> {
        toolchain {
            languageVersion = JavaLanguageVersion.of(21)
        }
    }

    tasks.withType<Test> {
        useJUnitPlatform()
        // docker-java(Testcontainers)가 기본 협상하는 Docker API 1.32는 최신 엔진
        // (OrbStack 등, 최소 1.40 요구)에서 거부된다. 최신 버전으로 고정한다.
        // Docker API는 하위 호환되므로 CI의 표준 Docker에서도 안전하다.
        systemProperty("api.version", "1.44")
    }
}
