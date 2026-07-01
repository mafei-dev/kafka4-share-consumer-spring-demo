package org.example.shareconsumertest.saga;

import java.lang.annotation.*;

/**
 * Framework-level marker for a saga event handler method.
 *
 * <p>This is <em>not</em> a Spring Kafka annotation. A method annotated with
 * {@code @SagaListener} carries only the framework-facing intent — the logical
 * saga name plus a couple of opt-in Kafka knobs. The rest of the Kafka wiring
 * (which topic to subscribe to, which consumer group) is decided entirely by the
 * framework at runtime in {@link SagaListenerRegistrar}, which registers an
 * equivalent {@code @KafkaListener}-style endpoint on your behalf.
 *
 * <p>So instead of "replacing this with {@code @KafkaListener} at compile time",
 * the framework <b>reads</b> this annotation at startup and <b>programmatically</b>
 * registers the real listener — the same mechanism Spring Kafka uses internally.
 *
 * <pre>{@code
 * @Component
 * public class OrderSaga {
 *     @SagaListener("order")
 *     public void onEvent(String payload) { ... }
 * }
 * }</pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface SagaListener {

    /**
     * Logical saga name. The framework derives Kafka wiring from this — e.g. the
     * consumer group id and the endpoint id — so no topic/group needs to be
     * spelled out at the call site.
     */
    String value();

    /**
     * Prefix prepended to the framework's base topic to form the topic this listener
     * subscribes to (final topic = {@code topicPrefix + KafkaShareConfig.TOPIC}). Empty
     * means no prefix, i.e. just the framework's base topic. Lets a developer namespace
     * the topic (e.g. per tenant/environment) while the framework still owns the rest.
     */
    String topicPrefix() default "";

    /**
     * Bean name of the {@link org.springframework.kafka.config.KafkaListenerContainerFactory}
     * to use, mirroring {@code @KafkaListener#containerFactory}. Empty means the framework
     * default ({@code "shareKafkaListenerContainerFactory"}). Only a
     * {@code ShareKafkaListenerContainerFactory} or {@code ConcurrentKafkaListenerContainerFactory}
     * is accepted; anything else fails fast at startup.
     */
    String containerFactory() default "";

    /**
     * Number of concurrent consumers, mirroring {@code @KafkaListener#concurrency}.
     * Empty means the container factory's default. Kept as a {@code String} to match
     * Spring's own annotation (and to leave room for property placeholders).
     */
    String concurrency() default "";
}
