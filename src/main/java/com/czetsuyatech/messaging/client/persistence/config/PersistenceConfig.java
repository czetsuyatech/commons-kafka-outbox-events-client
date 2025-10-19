package com.czetsuyatech.messaging.client.persistence.config;

import com.czetsuyatech.messaging.persistence.entities.UniKafkaEventEntitiesConfig;
import com.czetsuyatech.messaging.persistence.repositories.UniKafkaEventRepositoriesConfig;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.AuditorAware;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@EnableJpaRepositories(
    basePackageClasses = {
        UniKafkaEventRepositoriesConfig.class,
    })
@EntityScan(
    basePackageClasses = {
        UniKafkaEventEntitiesConfig.class
    })
@EnableTransactionManagement
@EnableJpaAuditing(auditorAwareRef = "auditorProvider")
public class PersistenceConfig {

  @Bean
  public AuditorAware<String> auditorProvider() {
    return new AuditorAwareImpl();
  }
}
