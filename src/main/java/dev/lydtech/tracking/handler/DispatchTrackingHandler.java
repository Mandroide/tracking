package dev.lydtech.tracking.handler;

import dev.lydtech.message.DispatchCompleted;
import dev.lydtech.message.DispatchPreparing;
import dev.lydtech.tracking.service.TrackingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;


@Slf4j
@RequiredArgsConstructor
@Component
@KafkaListener(id = "dispatchConsumerClient", topics = "dispatch.tracking",
        groupId = "dispatch.tracking.consumer", containerFactory = "kafkaListenerContainerFactory")
public class DispatchTrackingHandler {
    private final TrackingService trackingService;

    @KafkaHandler
    public void listen(DispatchPreparing dispatchPreparing) {
        try {
            trackingService.process(dispatchPreparing);
        } catch (ExecutionException | InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Processing failure", e);
        }
    }

    @KafkaHandler
    public void listen(DispatchCompleted dispatchCompleted) {
        try {
            trackingService.process(dispatchCompleted);
        } catch (ExecutionException | InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Processing failure", e);
        }
    }
}
