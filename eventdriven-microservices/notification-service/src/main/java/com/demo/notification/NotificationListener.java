package com.demo.notification;

import com.demo.common.Events.InventoryRejected;
import com.demo.common.Events.InventoryReserved;
import com.demo.common.Events.PaymentFailed;
import com.demo.common.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Terminal consumer: simulates emailing customers about final order outcomes. */
@Component
public class NotificationListener {
    private static final Logger log = LoggerFactory.getLogger(NotificationListener.class);

    @KafkaListener(topics = Topics.INVENTORY_RESERVED)
    void onReserved(InventoryReserved e) {
        log.info("EMAIL: order {} confirmed - thanks for your purchase!", e.orderId());
    }

    @KafkaListener(topics = Topics.PAYMENTS_FAILED)
    void onPaymentFailed(PaymentFailed e) {
        log.info("EMAIL: order {} cancelled - payment failed ({})", e.orderId(), e.reason());
    }

    @KafkaListener(topics = Topics.INVENTORY_REJECTED)
    void onRejected(InventoryRejected e) {
        log.info("EMAIL: order {} cancelled - {}; you have been refunded", e.orderId(), e.reason());
    }
}
