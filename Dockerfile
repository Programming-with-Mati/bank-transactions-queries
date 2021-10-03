FROM maven:3.8.1-adoptopenjdk-15 AS build-function
RUN mkdir /app
COPY . /app
WORKDIR /app
RUN mvn clean install -DskipTests

FROM openjdk:15-jdk
COPY --from=build-function /app/target/*.jar /app.jar
ENTRYPOINT ["java","-jar","/app.jar"]
