FROM maven:3.9-eclipse-temurin-17 AS build
ARG MODULE
WORKDIR /src
COPY . .
RUN mvn -q -pl ${MODULE} -am package -DskipTests

FROM eclipse-temurin:17-jre
ARG MODULE
WORKDIR /app
COPY --from=build /src/${MODULE}/target/${MODULE}-1.0.0.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
