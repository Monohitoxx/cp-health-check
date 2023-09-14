FROM eclipse-temurin:latest
WORKDIR /
ADD /build/libs/cp-health-check-0.0.1.jar cp-health-check.jar
CMD java -jar cp-health-check.jar