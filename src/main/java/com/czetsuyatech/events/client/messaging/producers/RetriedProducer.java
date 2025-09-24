package com.czetsuyatech.events.client.messaging.producers;

import com.czetsuyatech.events.client.messaging.constants.TopicKeys;
import com.czetsuyatech.events.config.UniKafkaEventAppConfig;
import com.czetsuyatech.events.messaging.producers.AbstractUniEventProducer;
import com.czetsuyatech.events.messaging.producers.UniEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class RetriedProducer extends AbstractUniEventProducer {

  public RetriedProducer(UniKafkaEventAppConfig appConfig,
      UniEventProducer uniEventProducer) {
    super(appConfig, uniEventProducer);
  }

  @Override
  protected String getTopicKey() {
    return TopicKeys.TOPIC_RETRIED;
  }
}
