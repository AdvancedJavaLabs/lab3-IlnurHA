package com.yourcompany;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

class ComparableKey implements WritableComparable<ComparableKey> {
    private Text category;
    private DoubleWritable value;

    public ComparableKey() {
        set(new DoubleWritable(), new Text());
    }
    
    public ComparableKey(double value, String category) {
        set(new DoubleWritable(value), new Text(category));
    }
    
    public void set(DoubleWritable value, Text category) {
        this.value = value;
        this.category = category;
    }

    public DoubleWritable getValue() { return value; }
    public Text getCategory() { return category; }
    
    @Override
    public void write(DataOutput out) throws IOException {
        value.write(out);
        category.write(out);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        value.readFields(in);
        category.readFields(in);
    }

    @Override
    public int compareTo(ComparableKey other) {
        int revenueCompare = -1 * value.compareTo(other.value);
        if (revenueCompare != 0) {
            return revenueCompare;
        }
        return category.compareTo(other.category);
    }
    @Override
    public int hashCode() {
        return category.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ComparableKey that = (ComparableKey) obj;
        return value.equals(that.value) && category.equals(that.category);
    }

    @Override
    public String toString() {
        return category + "," + value;
    }
}