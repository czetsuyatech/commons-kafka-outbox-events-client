package com.czetsuyatech.messaging.client.services;

public interface ProducerService {

  void processEvent();

  void failEvent();

  void ignoreEvent();

  void retryEvent();
}
