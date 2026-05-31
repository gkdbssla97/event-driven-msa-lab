FROM eclipse-temurin:21-jre

WORKDIR /app

COPY build/libs/websocket-service-0.0.1-SNAPSHOT.jar app.jar

EXPOSE 8083

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
