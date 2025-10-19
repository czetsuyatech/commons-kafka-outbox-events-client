package com.czetsuyatech.messaging.client.services.impl;

import com.czetsuyatech.messaging.client.messaging.producers.FailedProducer;
import com.czetsuyatech.messaging.client.messaging.producers.IgnoredProducer;
import com.czetsuyatech.messaging.client.messaging.producers.ProcessedProducer;
import com.czetsuyatech.messaging.client.messaging.producers.RetriedProducer;
import com.czetsuyatech.messaging.client.services.ProducerService;
import com.czetsuyatech.messaging.messaging.messages.UniEventDTO;
import java.time.Instant;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Slf4j
public class ProducerServiceImpl implements ProducerService {

  private final ProcessedProducer processedProducer;
  private final FailedProducer failedProducer;
  private final IgnoredProducer ignoredProducer;
  private final RetriedProducer retriedProducer;

  @Transactional
  @Override
  public void processEvent() {

    log.debug("Performing UC-1 - Sending / Consuming Ok");

    UniEventDTO uniEvent = getEvent();

    processedProducer.sendEvent(uniEvent);
  }

  @Transactional
  @Override
  public void failEvent() {

    log.debug("Performing UC-2 - Failed event");

    UniEventDTO uniEvent = getEvent();
    uniEvent.setEntityName("FAILED");
    uniEvent.setDescription("UC2 - Ko");

    failedProducer.sendEvent(uniEvent);
  }

  @Transactional
  @Override
  public void ignoreEvent() {

    log.debug("Performing UC-3 - Ignored event");

    UniEventDTO uniEvent = getEvent();
    uniEvent.setEntityName("XXX");
    uniEvent.setDescription("UC3 - Ignored");

    ignoredProducer.sendEvent(uniEvent);
  }

  @Transactional
  @Override
  public void retryEvent() {

    log.debug("Performing UC-4 - Retried event");

    UniEventDTO uniEvent = getEvent();
    uniEvent.setEntityName("RETRIED");
    uniEvent.setDescription("UC3 - Ignored");

    retriedProducer.sendEvent(uniEvent);
  }

  private UniEventDTO getEvent() {

    return UniEventDTO.builder()
        .eventId(UUID.randomUUID().toString())
        .parentEventId(null)
        .eventDate(Instant.now())
        .createdBy("SYSTEM")
        .eventType("TEST")
        .eventVersion(1)
        .eventSource("CzetsuyaTech")
        .entityName("PROCESSED")
        .callbackTopic(null)
        .entityData("{\"name\":\"Edward Legaspi\",\"alias\":\"czetsuya\",\"age\":39}")
        .description("UC1 - Sending / Consuming Ok")
        .build();
  }
}
