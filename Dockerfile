FROM eclipse-temurin:21-jdk AS build
WORKDIR /app

COPY .mvn .mvn
COPY mvnw pom.xml ./
COPY src src

RUN chmod +x mvnw && ./mvnw clean package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app

COPY --from=build /app/target/scalable-backend-system-0.0.1-SNAPSHOT.jar app.jar

EXPOSE 8082

CMD ["sh", "-c", "java -Dserver.port=${PORT:-8082} -jar /app/app.jar"]
