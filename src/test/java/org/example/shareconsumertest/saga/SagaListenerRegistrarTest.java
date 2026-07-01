package org.example.shareconsumertest.saga;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.ShareKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultShareConsumerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.TopicPartitionOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Tests the container-factory validation in {@link SagaListenerRegistrar}: only
 * {@link ShareKafkaListenerContainerFactory} and {@link ConcurrentKafkaListenerContainerFactory}
 * are allowed; anything else must fail fast at startup.
 *
 * <p>These are deliberately broker-free. The validation runs while the Spring context
 * initialises (inside {@code configureKafkaListeners}), long before any consumer connects,
 * so a plain {@link AnnotationConfigApplicationContext} with {@code autoStartup = false}
 * factories is enough — no Kafka broker or Testcontainers needed.
 */
class SagaListenerRegistrarTest {

    /**
     * Scenario 1 — both supported factory types are accepted, and each {@code @SagaListener}
     * ends up as a registered listener container.
     */
    @Test
    void supportedContainerFactories_areAccepted() {
        try (var context = new AnnotationConfigApplicationContext(SupportedFactoriesConfig.class)) {
            KafkaListenerEndpointRegistry registry = context.getBean(KafkaListenerEndpointRegistry.class);

            // Endpoint id is "saga-" + the annotation's value (see SagaListenerRegistrar).
            assertThat(registry.getListenerContainer("saga-shareOrder"))
                    .as("listener bound to a ShareKafkaListenerContainerFactory")
                    .isNotNull();
            assertThat(registry.getListenerContainer("saga-concurrentOrder"))
                    .as("listener bound to a ConcurrentKafkaListenerContainerFactory")
                    .isNotNull();
        }
    }

    /**
     * Scenario 2 — a factory that is neither of the two supported types is rejected, and
     * the context fails to start with our {@link IllegalStateException}.
     */
    @Test
    void unsupportedContainerFactory_isRejectedAtStartup() {
        Throwable thrown = catchThrowable(() ->
                new AnnotationConfigApplicationContext(UnsupportedFactoryConfig.class).close());

        assertThat(thrown).isNotNull();
        assertThat(NestedExceptionUtils.getMostSpecificCause(thrown))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("is not supported");
    }

    // ---------------------------------------------------------------------------------------
    // Test fixtures
    // ---------------------------------------------------------------------------------------

    /** Minimal consumer config; never actually connects because the factories don't auto-start. */
    private static Map<String, Object> consumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return props;
    }

    /** Two listeners, one on each supported factory type. */
    @Configuration(proxyBeanMethods = false)
    @Import(SagaListenerRegistrar.class)
    static class SupportedFactoriesConfig {

        @Bean
        ShareKafkaListenerContainerFactory<String, String> shareFactory() {
            var cf = new DefaultShareConsumerFactory<>(
                    consumerProps(), new StringDeserializer(), new StringDeserializer(), false);
            var factory = new ShareKafkaListenerContainerFactory<>(cf);
            factory.setAutoStartup(false);
            return factory;
        }

        @Bean
        ConcurrentKafkaListenerContainerFactory<String, String> concurrentFactory() {
            var cf = new DefaultKafkaConsumerFactory<>(
                    consumerProps(), new StringDeserializer(), new StringDeserializer());
            var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
            factory.setConsumerFactory(cf);
            factory.setAutoStartup(false);
            return factory;
        }

        @Bean
        SupportedSagas supportedSagas() {
            return new SupportedSagas();
        }
    }

    static class SupportedSagas {
        @SagaListener(value = "shareOrder", containerFactory = "shareFactory")
        void onShare(String payload) {
        }

        @SagaListener(value = "concurrentOrder", containerFactory = "concurrentFactory")
        void onConcurrent(String payload) {
        }
    }

    /** A single listener pointing at a factory that is neither Share nor Concurrent. */
    @Configuration(proxyBeanMethods = false)
    @Import(SagaListenerRegistrar.class)
    static class UnsupportedFactoryConfig {

        @Bean
        KafkaListenerContainerFactory<MessageListenerContainer> customFactory() {
            return new UnsupportedContainerFactory();
        }

        @Bean
        UnsupportedSaga unsupportedSaga() {
            return new UnsupportedSaga();
        }
    }

    static class UnsupportedSaga {
        @SagaListener(value = "bad", containerFactory = "customFactory")
        void onBad(String payload) {
        }
    }

    /** Stub factory of an unsupported type; methods are never invoked (validation throws first). */
    static class UnsupportedContainerFactory implements KafkaListenerContainerFactory<MessageListenerContainer> {
        @Override
        public MessageListenerContainer createListenerContainer(KafkaListenerEndpoint endpoint) {
            return null;
        }

        @Override
        public MessageListenerContainer createContainer(TopicPartitionOffset... topicPartitions) {
            return null;
        }

        @Override
        public MessageListenerContainer createContainer(String... topics) {
            return null;
        }

        @Override
        public MessageListenerContainer createContainer(Pattern topicPattern) {
            return null;
        }
    }
}
