# 运行阶段：采用极简的 JRE 镜像
FROM eclipse-temurin:11-jre-alpine
WORKDIR /app

# 直接将你在本地 target 目录下打好的 jar 包复制进容器
COPY target/java-executor-1.0.0.jar app.jar

# 8081是Spring Web端口，9999是XXL-Job底层Netty通信端口
EXPOSE 8081 9999

# 启动 Java 应用
ENTRYPOINT ["java", "-jar", "app.jar"]
