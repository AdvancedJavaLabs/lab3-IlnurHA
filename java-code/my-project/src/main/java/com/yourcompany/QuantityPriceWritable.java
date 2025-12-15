package com.yourcompany;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class QuantityPriceWritable implements Writable {
    private int quantity;
    private double price;

    public QuantityPriceWritable() {
        this.quantity = 0;
        this.price = 0.0;
    }

    public QuantityPriceWritable(int quantity, double single_unit_price) {
        this.quantity = quantity;
        this.price = single_unit_price * quantity;
    }

    public void merge(QuantityPriceWritable other) {
        this.quantity += other.quantity;
        this.price += other.price;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(price);
        out.writeInt(quantity);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        price = in.readDouble();
        quantity = in.readInt();
    }

    @Override
    public String toString() {
        return price + "," + quantity; // CSV format for output
    }
}
