package com.czetsuyatech.messaging.client.messaging.producers;

import com.czetsuyatech.messaging.client.messaging.constants.TopicKeys;
import com.czetsuyatech.messaging.config.UniKafkaEventAppConfig;
import com.czetsuyatech.messaging.messaging.producers.AbstractUniEventProducer;
import com.czetsuyatech.messaging.messaging.producers.UniEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class IgnoredProducer extends AbstractUniEventProducer {

  public IgnoredProducer(UniKafkaEventAppConfig appConfig,
      UniEventProducer uniEventProducer) {
    super(appConfig, uniEventProducer);
  }

  @Override
  protected String getTopicKey() {
    return TopicKeys.TOPIC_IGNORED;
  }
}
