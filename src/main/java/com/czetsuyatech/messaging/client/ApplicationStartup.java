package com.czetsuyatech.messaging.client;

import com.czetsuyatech.messaging.client.messaging.consumers.IgnoredConsumer;
import com.czetsuyatech.messaging.client.messaging.consumers.FailedConsumer;
import com.czetsuyatech.messaging.client.messaging.consumers.ProcessedConsumer;
import com.czetsuyatech.messaging.client.messaging.consumers.RetriedConsumer;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

@RequiredArgsConstructor
@Component
public class ApplicationStartup implements ApplicationListener<ApplicationReadyEvent> {

  private final ProcessedConsumer processedConsumer;
  private final FailedConsumer failedConsumer;
  private final IgnoredConsumer ignoredConsumer;
  private final RetriedConsumer retriedConsumer;

  @Override
  public void onApplicationEvent(ApplicationReadyEvent event) {

    processedConsumer.start();
    failedConsumer.start();
    ignoredConsumer.start();
    retriedConsumer.start();
  }
}
