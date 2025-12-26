package com.michalklempa.flink;

import java.util.Objects;

public class Product {
    public String id;
    public Long ingestTime;
    public Integer price;

    public Product() {
    }

    public Product(String id, Long ingestTime, Integer price) {
        this.id = id;
        this.ingestTime = ingestTime;
        this.price = price;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getIngestTime() {
        return ingestTime;
    }

    public void setIngestTime(Long ingestTime) {
        this.ingestTime = ingestTime;
    }

    public Integer getPrice() {
        return price;
    }

    public void setPrice(Integer price) {
        this.price = price;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Product product = (Product) o;
        return Objects.equals(id, product.id) && Objects.equals(ingestTime, product.ingestTime) && Objects.equals(price, product.price);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, ingestTime, price);
    }

    @Override
    public String toString() {
        return "{\"type\":\"Product\","
                + "\"id\":\"" + id + "\""
                + ", \"ingestTime\":\"" + ingestTime + "\""
                + ", \"price\":\"" + price + "\""
                + "}";
    }
}
