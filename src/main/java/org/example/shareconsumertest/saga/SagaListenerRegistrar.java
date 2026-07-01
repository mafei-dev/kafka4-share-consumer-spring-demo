package org.example.shareconsumertest.saga;

import java.lang.reflect.Method;
import java.util.Map;

import org.example.shareconsumertest.KafkaShareConfig;
import org.springframework.aop.support.AopUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListenerConfigurer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.config.ShareKafkaListenerContainerFactory;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;

import lombok.extern.slf4j.Slf4j;

/**
 * The heart of the "custom annotation" trick.
 *
 * <p>Spring Kafka exposes {@link KafkaListenerConfigurer}, a callback invoked once at
 * startup that hands you a {@link KafkaListenerEndpointRegistrar}. That registrar is the
 * exact same object Spring's own {@code @KafkaListener} post-processor feeds. So anything
 * we register here behaves identically to a method annotated with {@code @KafkaListener} —
 * we've just supplied the topic/group from framework logic instead of from annotation
 * attributes.
 *
 * <p>Flow: scan every bean for methods annotated with {@link SagaListener}, and for each
 * one build a {@link MethodKafkaListenerEndpoint} where:
 * <ul>
 *   <li>the framework decides the topic ({@link KafkaShareConfig#TOPIC}), the consumer
 *       group (derived from the saga name), and the endpoint id;</li>
 *   <li>the developer decides the container factory and concurrency via the annotation.</li>
 * </ul>
 *
 * <p>{@code @EnableKafka} is added defensively; Spring Boot's Kafka auto-configuration
 * already registers the listener infrastructure, and re-importing it is idempotent.
 */
@Slf4j
@Configuration(proxyBeanMethods = false)
@EnableKafka
public class SagaListenerRegistrar implements KafkaListenerConfigurer {

    /** Container factory used when the annotation does not name one. */
    private static final String DEFAULT_CONTAINER_FACTORY = "shareKafkaListenerContainerFactory";

    private final ApplicationContext applicationContext;

    /**
     * Resolves method arguments (e.g. a {@code String} payload) for the endpoints we register —
     * the same kind of factory the built-in {@code @KafkaListener} machinery uses. Built and
     * initialised here rather than exposed as a {@code @Bean}: this class is itself a
     * {@code @Configuration}, so injecting its own bean back into its constructor would be a
     * circular reference.
     */
    private final DefaultMessageHandlerMethodFactory messageHandlerMethodFactory;

    public SagaListenerRegistrar(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
        this.messageHandlerMethodFactory = new DefaultMessageHandlerMethodFactory();
        this.messageHandlerMethodFactory.afterPropertiesSet();
    }

    @Override
    public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
        for (String beanName : applicationContext.getBeanDefinitionNames()) {
            Object bean;
            try {
                bean = applicationContext.getBean(beanName);
            } catch (RuntimeException ex) {
                // Skip beans that can't be resolved eagerly (e.g. abstract/lazy factory beans).
                continue;
            }

            // Look through the *target* class so proxies (AOP, @Transactional, ...) don't hide methods.
            Class<?> targetClass = AopUtils.getTargetClass(bean);
            Map<Method, SagaListener> annotatedMethods = MethodIntrospector.selectMethods(
                    targetClass,
                    (MethodIntrospector.MetadataLookup<SagaListener>) method ->
                            AnnotatedElementUtils.findMergedAnnotation(method, SagaListener.class));

            annotatedMethods.forEach((method, sagaListener) ->
                    registerEndpoint(registrar, bean, method, sagaListener));
        }
    }

    private void registerEndpoint(KafkaListenerEndpointRegistrar registrar,
                                  Object bean, Method method, SagaListener sagaListener) {
        String saga = sagaListener.value();

        // Topic = developer-supplied prefix + framework's base topic; group is framework-owned.
        String topic = sagaListener.topicPrefix() + KafkaShareConfig.TOPIC;

        MethodKafkaListenerEndpoint<String, String> endpoint = new MethodKafkaListenerEndpoint<>();
        endpoint.setId("saga-" + saga);
        endpoint.setGroupId(KafkaShareConfig.GROUP + "-" + saga);
        endpoint.setTopics(topic);
        endpoint.setBean(bean);
        endpoint.setMethod(method);
        endpoint.setMessageHandlerMethodFactory(messageHandlerMethodFactory);

        // --- concurrency comes from the annotation (blank = factory default) ---
        String concurrency = sagaListener.concurrency();
        if (concurrency != null && !concurrency.isBlank()) {
            endpoint.setConcurrency(Integer.valueOf(concurrency.trim()));
        }

        // NOTE: endpoint.setConsumerProperties(Properties) exists and is the programmatic
        // equivalent of @KafkaListener(properties = ...), BUT the *share* container factory
        // ignores it. For extra consumer config, provide a custom container factory instead.

        // --- container factory comes from the annotation (blank = framework default) ---
        KafkaListenerContainerFactory<?> factory = resolveContainerFactory(sagaListener);

        // A share container invokes the share-specific listener contract, so the endpoint must
        // build a ShareRecordMessagingMessageListenerAdapter. This flag drives that choice;
        // without it the classic adapter throws "Container should never call this" at runtime.
        endpoint.setShareConsumer(factory instanceof ShareKafkaListenerContainerFactory);

        registrar.registerEndpoint(endpoint, factory);

        log.info("Registered @SagaListener '{}' -> topic '{}', group '{}', factory '{}', concurrency '{}' on {}#{}",
                saga, topic, endpoint.getGroupId(),
                factoryBeanName(sagaListener), concurrency.isBlank() ? "<default>" : concurrency,
                bean.getClass().getSimpleName(), method.getName());
    }

    private String factoryBeanName(SagaListener sagaListener) {
        String name = sagaListener.containerFactory();
        return (name == null || name.isBlank()) ? DEFAULT_CONTAINER_FACTORY : name.trim();
    }

    private KafkaListenerContainerFactory<?> resolveContainerFactory(SagaListener sagaListener) {
        String name = factoryBeanName(sagaListener);
        KafkaListenerContainerFactory<?> factory =
                applicationContext.getBean(name, KafkaListenerContainerFactory.class);

        // The framework only supports these two factory types; reject anything else fast.
        if (!(factory instanceof ShareKafkaListenerContainerFactory
                || factory instanceof ConcurrentKafkaListenerContainerFactory)) {
            throw new IllegalStateException(("@SagaListener container factory '%s' is a %s, which is not supported. "
                    + "Provide a %s or a %s.").formatted(
                    name, factory.getClass().getName(),
                    ShareKafkaListenerContainerFactory.class.getSimpleName(),
                    ConcurrentKafkaListenerContainerFactory.class.getSimpleName()));
        }
        return factory;
    }
}
