package ru.yandex.practicum.commerce.cart;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@SpringBootApplication
@EnableDiscoveryClient
@EnableTransactionManagement
@EnableFeignClients(basePackages = "ru.yandex.practicum.commerce.client")
public class ShoppingCartApplication {
    public static void main(String[] args) {
        SpringApplication.run(ShoppingCartApplication.class, args);
    }
}