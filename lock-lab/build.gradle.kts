// PgBouncer(transaction 모드) 뒤에서 잠금이 어떻게 동작하는지 재현하는 실험 모듈.
// 서비스 코드와 무관하며, 테스트가 곧 실험 기록이다.
plugins {
    id("io.spring.dependency-management")
}

dependencyManagement {
    imports {
        mavenBom("org.springframework.boot:spring-boot-dependencies:3.4.5")
    }
}

dependencies {
    implementation("org.postgresql:postgresql")

    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:postgresql")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-simple")
}

tasks.withType<Test> {
    testLogging {
        showStandardStreams = true
    }
}
