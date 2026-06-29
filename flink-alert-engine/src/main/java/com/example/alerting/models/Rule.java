package com.example.alerting.models;

public class Rule {
    public String productID;
    public int threshold;
    public int windowSeconds;
    public int ttlSeconds;

    public Rule() {}

    public Rule(String productID, int threshold, int windowSeconds, int ttlSeconds) {
        this.productID = productID;
        this.threshold = threshold;
        this.windowSeconds = windowSeconds;
        this.ttlSeconds = ttlSeconds;
    }

    @Override
    public String toString() {
        return "Rule{" +
                "productID='" + productID + '\'' +
                ", threshold=" + threshold +
                ", windowSeconds=" + windowSeconds +
                ", ttlSeconds=" + ttlSeconds +
                '}';
    }
}
