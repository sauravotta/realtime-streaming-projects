package com.demo.inventory;

import com.demo.common.Events.InventoryRejected;
import com.demo.common.Events.InventoryReserved;
import com.demo.common.Events.PaymentCompleted;
import com.demo.common.Topics;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class InventoryListener {
    private static final Logger log = LoggerFactory.getLogger(InventoryListener.class);

    private final Map<String, Integer> stock = new ConcurrentHashMap<>(Map.of("BOOK-1", 10, "PHONE-1", 2, "LAPTOP-1", 0));
    private final KafkaTemplate<String, Object> kafka;

    public InventoryListener(KafkaTemplate<String, Object> kafka) {
        this.kafka = kafka;
    }

    @KafkaListener(topics = Topics.PAYMENTS_COMPLETED)
    void onPaymentCompleted(PaymentCompleted e) {
        if (tryReserve(e)) {
            log.info("Reserved {} x {} for order {}", e.quantity(), e.sku(), e.orderId());
            kafka.send(Topics.INVENTORY_RESERVED, e.orderId(), new InventoryReserved(e.orderId()));
        } else {
            log.warn("Insufficient stock of {} for order {}", e.sku(), e.orderId());
            kafka.send(Topics.INVENTORY_REJECTED, e.orderId(), new InventoryRejected(e.orderId(), "insufficient stock for " + e.sku()));
        }
    }

    private boolean tryReserve(PaymentCompleted e) {
        boolean[] ok = {false};
        stock.computeIfPresent(e.sku(), (k, v) -> {
            if (v >= e.quantity()) {
                ok[0] = true;
                return v - e.quantity();
            }
            return v;
        });
        return ok[0];
    }
}
