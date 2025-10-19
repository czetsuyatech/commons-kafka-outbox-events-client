package com.czetsuyatech.messaging.client.messaging.consumers;

import com.czetsuyatech.messaging.client.messaging.constants.TopicKeys;
import com.czetsuyatech.messaging.config.UniKafkaEventAppConfig;
import com.czetsuyatech.messaging.mappers.EventMapper;
import com.czetsuyatech.messaging.messaging.consumers.UniEventConsumer;
import com.czetsuyatech.messaging.messaging.exceptions.EventFailedException;
import com.czetsuyatech.messaging.messaging.exceptions.EventRetryableException;
import com.czetsuyatech.messaging.messaging.messages.UniEventDTO;
import com.czetsuyatech.messaging.services.UniDeadLetterService;
import com.czetsuyatech.messaging.services.UniInboundEventService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
@Slf4j
public class FailedConsumer extends UniEventConsumer {

  private final ObjectMapper om;

  public FailedConsumer(
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

    return uniEvent.getEntityName().startsWith("FAILED")
        ? true
        : false;
  }

  @Override
  protected String getTopicKey() {
    return TopicKeys.TOPIC_FAILED;
  }

  @Override
  protected void preConsumeMessage() {

    log.debug("Before consuming the message");
  }

  @Override
  protected void handleMessage(UniEventDTO uniEvent) throws EventRetryableException, EventFailedException {

    log.info("Handling message={}", uniEvent);

    throw new EventFailedException("KO", "Failed");
  }

  @Override
  public ObjectMapper getObjectMapper() {
    return om;
  }
}
