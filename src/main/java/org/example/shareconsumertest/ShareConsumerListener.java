package org.example.shareconsumertest;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * Consumes from {@link KafkaShareConfig#TOPIC} using a Kafka 4 share group.
 *
 * <p>The {@code containerFactory} points at the {@link org.springframework.kafka.config.ShareKafkaListenerContainerFactory}
 * declared in {@link KafkaShareConfig}; that is what makes this a share consumer
 * rather than a classic one. With the default (implicit) share ack mode, each record
 * is acknowledged automatically once this method returns normally.
 *
 * <p>Received values are buffered in a queue purely so tests can await delivery.
 */
@Slf4j
@Component
public class ShareConsumerListener {

    private final BlockingQueue<String> received = new LinkedBlockingQueue<>();

    @KafkaListener(
            topics = KafkaShareConfig.TOPIC,
            groupId = KafkaShareConfig.GROUP,
            containerFactory = "shareKafkaListenerContainerFactory")
    public void listen(String message) {
        log.info("Share consumer received: {}", message);
        received.add(message);
    }

    /** Buffer of values received so far; used by tests to await delivery. */
    public BlockingQueue<String> getReceived() {
        return received;
    }
}
