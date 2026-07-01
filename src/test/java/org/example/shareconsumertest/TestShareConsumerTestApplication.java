package org.example.shareconsumertest;

import org.springframework.boot.SpringApplication;

public class TestShareConsumerTestApplication {

    public static void main(String[] args) {
        SpringApplication.from(ShareConsumerTestApplication::main).with(TestcontainersConfiguration.class).run(args);
    }

}
