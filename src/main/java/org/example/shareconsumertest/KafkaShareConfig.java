package org.example.shareconsumertest;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ShareKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultShareConsumerFactory;
import org.springframework.kafka.core.ShareConsumerFactory;

import org.apache.kafka.clients.admin.NewTopic;

/**
 * Wiring for a Kafka 4 <b>share consumer</b> (KIP-932, "Queues for Kafka").
 *
 * <p>Unlike a classic consumer group where each partition is owned by exactly one
 * member, a <em>share group</em> lets multiple consumers cooperatively read from the
 * same partitions with per-record acknowledgement — giving queue-like semantics on
 * top of a Kafka topic.
 *
 * <p>Spring Boot 4.1 does not auto-configure the share-consumer beans, so we declare
 * a {@link ShareConsumerFactory} and a {@link ShareKafkaListenerContainerFactory}
 * here. The producer side ({@code KafkaTemplate}) is still auto-configured by Boot.
 */
@Configuration(proxyBeanMethods = false)
public class KafkaShareConfig {

    public static final String TOPIC = "share-consumer-demo";
    public static final String GROUP = "share-consumer-demo-group";

    /** Auto-created on startup by the auto-configured {@code KafkaAdmin}. */
    @Bean
    public NewTopic shareTopic() {
        return TopicBuilder.name(TOPIC)
                .partitions(2)
                .replicas(1)
                .build();
    }

    /**
     * Factory that builds {@link org.apache.kafka.clients.consumer.ShareConsumer}
     * instances. Bootstrap servers come from {@link KafkaConnectionDetails} so this
     * works both for a normally-running app and for the Testcontainers broker wired
     * in via {@code @ServiceConnection}.
     */
    @Bean
    public ShareConsumerFactory<String, String> shareConsumerFactory(KafkaConnectionDetails connectionDetails) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                String.join(",", connectionDetails.getBootstrapServers()));
        // Deserializers are passed explicitly, so tell the factory not to configure its own.
        return new DefaultShareConsumerFactory<>(props, new StringDeserializer(), new StringDeserializer(), false);
    }

    /**
     * Container factory referenced by {@code @KafkaListener(containerFactory = ...)}.
     * It produces {@link org.springframework.kafka.listener.ShareKafkaMessageListenerContainer}
     * instances driven by the share consumer above.
     */
    @Bean
    public ShareKafkaListenerContainerFactory<String, String> shareKafkaListenerContainerFactory(
            ShareConsumerFactory<String, String> shareConsumerFactory) {
        return new ShareKafkaListenerContainerFactory<>(shareConsumerFactory);
    }
}
