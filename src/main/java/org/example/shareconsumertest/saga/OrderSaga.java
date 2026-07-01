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

    @SagaListener(topicPrefix = "y1", value = "order-1", containerFactory = "shareKafkaListenerContainerFactory", concurrency = "10")
    public void onEvent(String payload) {
        log.info("y1:OrderSaga handled event: {}", payload);
    }

    @SagaListener(topicPrefix = "y2", value = "order-2", containerFactory = "shareKafkaListenerContainerFactory", concurrency = "10")
    public void onEvent1(String payload) {
        log.info("y2:OrderSaga handled event: {}", payload);
    }

    @SagaListener(topicPrefix = "y3", value = "order-3", containerFactory = "concurrentKafkaListenerContainerFactory", concurrency = "10")
    public void onEvent2(String payload) {
        log.info("y3:OrderSaga handled event: {}", payload);
    }


}
