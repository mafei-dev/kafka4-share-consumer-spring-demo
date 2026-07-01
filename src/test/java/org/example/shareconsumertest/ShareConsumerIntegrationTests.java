package org.example.shareconsumertest;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Sends a message to the share topic and verifies the Kafka 4 share consumer
 * ({@link ShareConsumerListener}) receives it.
 *
 * <p>The {@code @KafkaListener} share container is auto-started with the Spring
 * context, so by the time this test sends a record the share group is already
 * initialised on the (empty) topic — the record is then delivered to the share
 * consumer.
 */
@Import(TestcontainersConfiguration.class)
@SpringBootTest
class ShareConsumerIntegrationTests {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ShareConsumerListener shareConsumerListener;

    @Test
    void messageSentToTopicIsReceivedByShareConsumer() throws Exception {
        String payload = "hello-share-consumer";

        // Produce with the auto-configured KafkaTemplate; block until the broker acks.
        kafkaTemplate.send(KafkaShareConfig.TOPIC, "demo-key", payload).get(10, TimeUnit.SECONDS);

        // Await delivery to the share consumer.
        String received = shareConsumerListener.getReceived().poll(30, TimeUnit.SECONDS);

        assertNotNull(received, "Share consumer did not receive the message within the timeout");
        assertEquals(payload, received);
    }
}
