package com.yourcompany;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

public class SortByValue extends WritableComparator {
       
    protected SortByValue() {
        super(ComparableKey.class, true);
    }
    
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        ComparableKey key1 = (ComparableKey) a;
        ComparableKey key2 = (ComparableKey) b;        
        return key1.compareTo(key2);
    }
}
