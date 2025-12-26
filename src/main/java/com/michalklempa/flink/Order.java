package com.michalklempa.flink;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class Order {
    public String id;
    public Long ingestTime;
    public List<Product> productList;

    public Order() {
    }

    public Order(String id, Long ingestTime, List<Product> productList) {
        this.id = id;
        this.ingestTime = ingestTime;
        this.productList = productList;
    }

    public Long getIngestTime() {
        return ingestTime;
    }

    public void setIngestTime(Long ingestTime) {
        this.ingestTime = ingestTime;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<Product> getProductList() {
        return productList;
    }

    public void setProductList(List<Product> productList) {
        this.productList = productList;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Order order = (Order) o;
        return Objects.equals(ingestTime, order.ingestTime) && Objects.equals(id, order.id) && Objects.equals(productList, order.productList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ingestTime, id, productList);
    }

    @Override
    public String toString() {
        return "{\"type\":\"Order\","
                + "\"ingestTime\":\"" + ingestTime + "\""
                + ", \"id\":\"" + id + "\""
                + ", \"productList\":" + productList
                + "}";
    }
}
