package com.yourcompany;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SortingPartitioner extends Partitioner<ComparableKey, IntWritable> {
    @Override
    public int getPartition(ComparableKey key, IntWritable value, int numPartitions) {
        return Math.abs(key.getCategory().toString().hashCode()) % numPartitions;
    }
}
