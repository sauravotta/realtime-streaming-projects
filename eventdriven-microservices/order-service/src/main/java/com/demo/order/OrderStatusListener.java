package com.demo.order;

import com.demo.common.Events.InventoryRejected;
import com.demo.common.Events.InventoryReserved;
import com.demo.common.Events.PaymentCompleted;
import com.demo.common.Events.PaymentFailed;
import com.demo.common.Topics;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Updates order status as the saga progresses. */
@Component
public class OrderStatusListener {

    private final OrderController orders;

    public OrderStatusListener(OrderController orders) {
        this.orders = orders;
    }

    @KafkaListener(topics = Topics.PAYMENTS_COMPLETED)
    void onPaymentCompleted(PaymentCompleted e) {
        orders.update(e.orderId(), "PAID", "Payment completed, reserving inventory");
    }

    @KafkaListener(topics = Topics.PAYMENTS_FAILED)
    void onPaymentFailed(PaymentFailed e) {
        orders.update(e.orderId(), "CANCELLED", "Payment failed: " + e.reason());
    }

    @KafkaListener(topics = Topics.INVENTORY_RESERVED)
    void onReserved(InventoryReserved e) {
        orders.update(e.orderId(), "CONFIRMED", "Inventory reserved");
    }

    @KafkaListener(topics = Topics.INVENTORY_REJECTED)
    void onRejected(InventoryRejected e) {
        orders.update(e.orderId(), "CANCELLED", "Inventory rejected: " + e.reason() + " (payment refunded)");
    }
}
