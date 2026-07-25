# 编译阶段：利用 Maven 官方镜像进行打包
FROM maven:3.8.6-openjdk-11-slim AS builder
WORKDIR /app
COPY pom.xml .
# 预先下载依赖，利用 Docker 缓存加速构建
RUN mvn dependency:go-offline

COPY src ./src
# 编译并打包为可执行的 jar
RUN mvn clean package -DskipTests

# 运行阶段：采用极简的 JRE 镜像，剥离源码和编译环境
FROM openjdk:11-jre-slim
WORKDIR /app
COPY --from=builder /app/target/java-executor-1.0.0.jar app.jar

# 8081是Spring Web端口，9999是XXL-Job底层Netty通信端口
EXPOSE 8081 9999

# 启动 Java 应用
ENTRYPOINT ["java", "-jar", "app.jar"]
