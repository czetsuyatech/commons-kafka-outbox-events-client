package com.czetsuyatech.events.client.messaging.consumers;

import com.czetsuyatech.events.client.messaging.constants.TopicKeys;
import com.czetsuyatech.events.config.UniKafkaEventAppConfig;
import com.czetsuyatech.events.mappers.EventMapper;
import com.czetsuyatech.events.messaging.consumers.UniEventConsumer;
import com.czetsuyatech.events.messaging.exceptions.EventFailedException;
import com.czetsuyatech.events.messaging.exceptions.EventRetryableException;
import com.czetsuyatech.events.messaging.messages.UniEventDTO;
import com.czetsuyatech.events.services.UniDeadLetterService;
import com.czetsuyatech.events.services.UniInboundEventService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
@Slf4j
public class ProcessedConsumer extends UniEventConsumer {

  private final ObjectMapper om;

  public ProcessedConsumer(
      UniKafkaEventAppConfig appConfig,
      ConsumerFactory<String, String> consumerFactory,
      UniInboundEventService uniInboundEventService,
      UniDeadLetterService uniDeadLetterService,
      EventMapper eventMapper,
      ObjectMapper om) {

    super(appConfig, consumerFactory, uniInboundEventService, uniDeadLetterService, eventMapper);

    this.om = om;
  }

  @Override
  protected boolean filterEvent(UniEventDTO uniEvent) {

    log.debug("Filtering event");

    return uniEvent.getEntityName().equals("PROCESSED")
        ? true
        : false;
  }

  @Override
  protected String getTopicKey() {
    return TopicKeys.TOPIC_PROCESSED;
  }

  @Override
  protected void preConsumeMessage() {

    log.debug("Before consuming the message");
  }

  @Override
  protected void handleMessage(UniEventDTO uniEvent) throws EventRetryableException, EventFailedException {
    log.info("Handling message={}", uniEvent);
  }

  @Override
  public ObjectMapper getObjectMapper() {
    return om;
  }
}
