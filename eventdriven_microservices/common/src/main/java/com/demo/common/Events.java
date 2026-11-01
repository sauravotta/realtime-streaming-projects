package com.demo.common;

import java.math.BigDecimal;

/** Event payloads exchanged over Kafka (JSON). */
public final class Events {

    public record OrderCreated(String orderId, String customerId, String sku, int quantity, BigDecimal amount) {
    }

    public record PaymentCompleted(String orderId, String sku, int quantity, BigDecimal amount) {
    }

    public record PaymentFailed(String orderId, String reason) {
    }

    public record InventoryReserved(String orderId) {
    }

    public record InventoryRejected(String orderId, String reason) {
    }

    private Events() {
    }
}
