plugins {
    `java-library`
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.18.3")
    testImplementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.18.3")
}
