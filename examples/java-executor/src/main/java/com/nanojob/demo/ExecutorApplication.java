package com.nanojob.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ExecutorApplication {
    public static void main(String[] args) {
        SpringApplication.run(ExecutorApplication.class, args);
        System.out.println("✅ Java Executor 启动成功！每隔 30 秒会自动向 NanoJob 引擎发送心跳报到...");
    }
}
