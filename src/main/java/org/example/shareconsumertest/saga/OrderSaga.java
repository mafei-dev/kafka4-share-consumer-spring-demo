package org.example.shareconsumertest.saga;

import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * Demonstrates the custom annotation. There is no {@code @KafkaListener} here — just the
 * framework's {@link SagaListener}. At startup {@link SagaListenerRegistrar} finds this
 * method and registers a real (share-consumer) Kafka listener for it, choosing the topic
 * and group itself while honouring the container factory and concurrency from the annotation.
 *
 * <p>The parameter is a plain {@code String}, which is resolved as the record's value
 * (payload) by the message handler factory. If you need the raw
 * {@code ConsumerRecord}/headers, that requires the Kafka-aware argument resolvers — see
 * the note in {@link SagaListenerRegistrar}.
 */
@Slf4j
@Component
public class OrderSaga {

    @SagaListener(value = "order", containerFactory = "shareKafkaListenerContainerFactory", concurrency = "10")
    public void onEvent(String payload) {
        log.info("OrderSaga handled event: {}", payload);
    }
}
