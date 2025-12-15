FROM openjdk:11.0.4-jre-slim
WORKDIR /app
# Copy the application jar and its dependencies
# COPY target/dependency-jars ./dependency-jars
COPY target/*.jar ./app.jar
# The command to run your job
CMD ["java", "-cp", "app.jar", "com.yourcompany.CategoryDriver", "hdfs://namenode:9000/input", "hdfs://namenode:9000/output"]
