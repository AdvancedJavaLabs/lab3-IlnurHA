package com.yourcompany;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortingGrouping extends WritableComparator {
    protected SortingGrouping() {
        super(ComparableKey.class, true);
    }
        
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        ComparableKey key1 = (ComparableKey) a;
        ComparableKey key2 = (ComparableKey) b;
        return key1.getCategory().compareTo(key2.getCategory());
    }
}
