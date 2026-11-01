package com.demo.payment;

import com.demo.common.Events.InventoryRejected;
import com.demo.common.Events.OrderCreated;
import com.demo.common.Events.PaymentCompleted;
import com.demo.common.Events.PaymentFailed;
import com.demo.common.Topics;
import java.math.BigDecimal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaymentListener {
    private static final Logger log = LoggerFactory.getLogger(PaymentListener.class);

    private final KafkaTemplate<String, Object> kafka;
    private final BigDecimal limit;

    public PaymentListener(KafkaTemplate<String, Object> kafka,
                           @Value("${payment.limit:1000}") BigDecimal limit) {
        this.kafka = kafka;
        this.limit = limit;
    }

    @KafkaListener(topics = Topics.ORDERS_CREATED)
    void onOrderCreated(OrderCreated e) {
        if (e.amount().compareTo(limit) > 0) {
            log.warn("Payment declined for order {}: amount {} exceeds limit {}", e.orderId(), e.amount(), limit);
            kafka.send(Topics.PAYMENTS_FAILED, e.orderId(), new PaymentFailed(e.orderId(), "amount exceeds limit " + limit));
            return;
        }
        log.info("Payment charged for order {}: {}", e.orderId(), e.amount());
        kafka.send(Topics.PAYMENTS_COMPLETED, e.orderId(),
                new PaymentCompleted(e.orderId(), e.sku(), e.quantity(), e.amount()));
    }

    /** Compensation step of the saga. */
    @KafkaListener(topics = Topics.INVENTORY_REJECTED)
    void onInventoryRejected(InventoryRejected e) {
        log.info("Refunding payment for order {} ({})", e.orderId(), e.reason());
    }
}
