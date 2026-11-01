package com.demo.order;

import com.demo.common.Events.OrderCreated;
import com.demo.common.Topics;
import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OrderController {

    public record CreateOrderRequest(String customerId, String sku, int quantity, BigDecimal amount) {
    }

    public record OrderView(String orderId, String status, String detail) {
    }

    private final Map<String, OrderView> orders = new ConcurrentHashMap<>();
    private final KafkaTemplate<String, Object> kafka;

    public OrderController(KafkaTemplate<String, Object> kafka) {
        this.kafka = kafka;
    }

    @PostMapping("/orders")
    public ResponseEntity<OrderView> create(@RequestBody CreateOrderRequest req) {
        if (req.quantity() <= 0 || req.amount() == null || req.amount().signum() <= 0) {
            return ResponseEntity.badRequest().build();
        }
        String id = UUID.randomUUID().toString();
        var view = new OrderView(id, "PENDING", "Order created");
        orders.put(id, view);
        kafka.send(Topics.ORDERS_CREATED, id,
                new OrderCreated(id, req.customerId(), req.sku(), req.quantity(), req.amount()));
        return ResponseEntity.accepted().body(view);
    }

    @GetMapping("/orders/{id}")
    public ResponseEntity<OrderView> get(@PathVariable String id) {
        var view = orders.get(id);
        return view == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(view);
    }

    void update(String id, String status, String detail) {
        orders.put(id, new OrderView(id, status, detail));
    }
}
