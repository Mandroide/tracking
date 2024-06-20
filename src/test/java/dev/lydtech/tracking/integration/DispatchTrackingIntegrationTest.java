package dev.lydtech.tracking.integration;

import dev.lydtech.TrackingConfiguration;
import dev.lydtech.message.DispatchCompleted;
import dev.lydtech.message.DispatchPreparing;
import dev.lydtech.tracking.message.TrackingStatusUpdated;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.time.LocalDate;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

@SpringBootTest(classes = TrackingConfiguration.class)
@ActiveProfiles("test")
@Slf4j
@EmbeddedKafka(kraft = true, controlledShutdown = true, bootstrapServersProperty = "kafka.bootstrap-servers")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class DispatchTrackingIntegrationTest {
    private static final String DISPATCH_TRACKING_TOPIC = "dispatch.tracking";
    private static final String TRACKING_STATUS_TOPIC = "tracking.status";
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired
    private EmbeddedKafkaKraftBroker embeddedKafkaKraftBroker;
    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
    @Autowired
    private KafkaTestListener kafkaTestListener;

    @Component
    private static class KafkaTestListener {
        AtomicInteger trackingStatusCounter = new AtomicInteger(0);

        @KafkaListener(groupId = "KafkaIntegrationTest", topics = TRACKING_STATUS_TOPIC)
        void receiveTrackingStatusUpdated(@Payload TrackingStatusUpdated trackingStatusUpdated) {
            log.debug("Received tracking status updated for {}", trackingStatusUpdated);
            trackingStatusCounter.incrementAndGet();
        }
    }

    @BeforeEach
    void setUp() {
        kafkaTestListener.trackingStatusCounter.set(0);

        kafkaListenerEndpointRegistry.getListenerContainers().forEach(c ->
                ContainerTestUtils.waitForAssignment(c, embeddedKafkaKraftBroker.getPartitionsPerTopic()));
    }

    @Test
    void testTrackingStatusPreparingFlow() {
        DispatchPreparing dispatchPreparing = DispatchPreparing.builder().orderId(UUID.randomUUID()).build();
        kafkaTemplate.send(DISPATCH_TRACKING_TOPIC, dispatchPreparing);

        await().atMost(2L, TimeUnit.SECONDS).pollDelay(100L, TimeUnit.MILLISECONDS)
                .untilAtomic(kafkaTestListener.trackingStatusCounter, equalTo(1));
    }

    @Test
    void testTrackingStatusCompletedFlow() {
        DispatchCompleted dispatchCompleted = DispatchCompleted.builder().orderId(UUID.randomUUID()).completionDate(LocalDate.now()).build();
        kafkaTemplate.send(DISPATCH_TRACKING_TOPIC, dispatchCompleted);

        await().atMost(2L, TimeUnit.SECONDS).pollDelay(100L, TimeUnit.MILLISECONDS)
                .untilAtomic(kafkaTestListener.trackingStatusCounter, equalTo(1));
    }
}
