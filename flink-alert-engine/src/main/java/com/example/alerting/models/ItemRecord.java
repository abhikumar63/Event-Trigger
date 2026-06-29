package com.example.alerting.models;

public class ItemRecord {
    public String productId;
    public long timestamp;
    public int quantity;

    public ItemRecord() {}

    public ItemRecord(String productId, long timestamp, int quantity) {
        this.productId = productId;
        this.timestamp = timestamp;
        this.quantity = quantity;
    }

    @Override
    public String toString() {
        return "ItemRecord{" +
                "productId='" + productId + '\'' +
                ", timestamp=" + timestamp +
                ", quantity=" + quantity +
                '}';
    }
}
