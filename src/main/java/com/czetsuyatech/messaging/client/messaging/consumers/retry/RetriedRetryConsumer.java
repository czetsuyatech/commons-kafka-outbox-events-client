package com.czetsuyatech.messaging.client.messaging.consumers.retry;

import com.czetsuyatech.messaging.client.messaging.constants.TopicKeys;
import com.czetsuyatech.messaging.client.messaging.consumers.RetriedConsumer;
import com.czetsuyatech.messaging.config.UniKafkaEventAppConfig;
import com.czetsuyatech.messaging.mappers.EventMapper;
import com.czetsuyatech.messaging.messaging.consumers.UniEventRetryConsumer;
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
  public void handleMessage(UniEventDTO uniEvent) throws EventRetryableException, EventFailedException {

    log.info("Handling retry message={}", uniEvent);

    retriedConsumer.handleMessage(uniEvent);
  }

  @Override
  public ObjectMapper getObjectMapper() {
    return null;
  }
}
