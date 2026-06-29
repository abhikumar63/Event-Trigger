package com.example.alerting.models;

public class AlertTriggered {
    public String productID;
    public int ttlSeconds;
    public int count;

    public AlertTriggered() {}

    public AlertTriggered(String productID, int ttlSeconds, int count) {
        this.productID = productID;
        this.ttlSeconds = ttlSeconds;
        this.count = count;
    }

    @Override
    public String toString() {
        return "AlertTriggered{" +
                "productID='" + productID + '\'' +
                ", ttlSeconds=" + ttlSeconds +
                ", count=" + count +
                '}';
    }
}
