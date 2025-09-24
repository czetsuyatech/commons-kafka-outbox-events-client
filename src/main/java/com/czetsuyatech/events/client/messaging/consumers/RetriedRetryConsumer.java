package com.czetsuyatech.events.client.messaging.consumers;

import com.czetsuyatech.events.client.messaging.constants.TopicKeys;
import com.czetsuyatech.events.config.UniKafkaEventAppConfig;
import com.czetsuyatech.events.mappers.EventMapper;
import com.czetsuyatech.events.messaging.consumers.UniEventRetryConsumer;
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
public class RetriedRetryConsumer extends UniEventRetryConsumer {

  private final ObjectMapper om;
  private final RetriedConsumer retriedConsumer;

  public RetriedRetryConsumer(
      UniKafkaEventAppConfig appConfig,
      ConsumerFactory<String, String> consumerFactory,
      UniInboundEventService uniInboundEventService,
      UniDeadLetterService uniDeadLetterService,
      EventMapper eventMapper,
      ObjectMapper om,
      RetriedConsumer retriedConsumer) {

    super(appConfig, consumerFactory, uniInboundEventService, uniDeadLetterService, eventMapper);

    this.retriedConsumer = retriedConsumer;
    this.om = om;
  }

  @Override
  protected String getTopicKey() {
    return TopicKeys.TOPIC_RETRIED_RETRY;
  }

  @Override
  protected void preConsumeMessage() {

  }

  @Override
  protected void handleMessage(UniEventDTO uniEvent) throws EventRetryableException, EventFailedException {

    log.info("Handling retry message={}", uniEvent);

    retriedConsumer.handleMessage(uniEvent);
  }

  @Override
  public ObjectMapper getObjectMapper() {
    return null;
  }
}
