package com.demo.common;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaAdmin.NewTopics;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/** Shared config: topic creation and retry + dead-letter handling. Import from each service. */
@Configuration
public class KafkaCommonConfig {

    @Bean
    public NewTopics demoTopics() {
        return new NewTopics(
                topic(Topics.ORDERS_CREATED), topic(Topics.PAYMENTS_COMPLETED), topic(Topics.PAYMENTS_FAILED),
                topic(Topics.INVENTORY_RESERVED), topic(Topics.INVENTORY_REJECTED));
    }

    private static org.apache.kafka.clients.admin.NewTopic topic(String name) {
        return TopicBuilder.name(name).partitions(3).replicas(1).build();
    }

    /** Retry 3 times, 1s apart, then publish the record to <topic>.DLT. */
    @Bean
    public CommonErrorHandler errorHandler(KafkaOperations<Object, Object> template) {
        var recoverer = new DeadLetterPublishingRecoverer(template,
                (record, ex) -> new TopicPartition(record.topic() + ".DLT", -1));
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3));
    }
}
