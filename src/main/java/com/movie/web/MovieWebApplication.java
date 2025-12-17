package com.movie.web;                              // 定义包名

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Spring Boot主应用
 */
@SpringBootApplication(scanBasePackages = "com.movie.web")
public class MovieWebApplication {
    public static void main(String[] args) {
        SpringApplication.run(MovieWebApplication.class, args);
    }
}
