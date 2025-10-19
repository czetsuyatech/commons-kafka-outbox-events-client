package com.czetsuyatech.messaging.client.web.controllers;

import com.czetsuyatech.messaging.client.services.ProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
@RequiredArgsConstructor
public class EventsController {

  private final ProducerService producerService;

  @GetMapping("/events/processed")
  public void processEvent() {

    log.info("Receive uc-1 request");

    producerService.processEvent();
  }

  @GetMapping("/events/failed")
  public void failEvent() {

    log.info("Receive uc-2 request");

    producerService.failEvent();
  }

  @GetMapping("/events/ignored")
  public void ignoreEvent() {

    log.info("Receive uc-3 request");

    producerService.ignoreEvent();
  }

  @GetMapping("/events/retried")
  public void retryEvent() {

    log.info("Receive uc-4 request");

    producerService.retryEvent();
  }
}
