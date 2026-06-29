package com.example.alerting.models;

import java.util.List;

public class OrderEvent {
    public String orderId;
    public long timestamp;
    public List<Item> items;

    public static class Item {
        public String productId;
        public int quantity;

        public Item() {}

        public Item(String productId, int quantity) {
            this.productId = productId;
            this.quantity = quantity;
        }

        @Override
        public String toString() {
            return "Item{" +
                    "productId='" + productId + '\'' +
                    ", quantity=" + quantity +
                    '}';
        }
    }

    public OrderEvent() {}

    public OrderEvent(String orderId, long timestamp, List<Item> items) {
        this.orderId = orderId;
        this.timestamp = timestamp;
        this.items = items;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId='" + orderId + '\'' +
                ", timestamp=" + timestamp +
                ", items=" + items +
                '}';
    }
}
