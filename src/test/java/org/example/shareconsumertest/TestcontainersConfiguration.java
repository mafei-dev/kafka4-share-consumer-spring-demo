package org.example.shareconsumertest;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@TestConfiguration(proxyBeanMethods = false)
class TestcontainersConfiguration {

    @Bean
    @ServiceConnection
    KafkaContainer kafkaContainer() {
        return new KafkaContainer(DockerImageName.parse("apache/kafka-native:latest"))
                .withReuse(true)
                // Give the container a fixed, recognisable name.
                .withCreateContainerCmdModifier(cmd -> cmd.withName("share-consumer-kafka"))
                // Share groups (KIP-932) are an opt-in feature and are disabled on the
                // broker by default. Enable the share group coordinator and its state
                // topic (single-broker settings) so share consumers can be used.
                .withEnv("KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS", "classic,consumer,share")
                .withEnv("KAFKA_GROUP_SHARE_ENABLE", "true")
                .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR", "1")
                .withEnv("KAFKA_UNSTABLE_API_VERSIONS_ENABLE", "true");
    }

}
