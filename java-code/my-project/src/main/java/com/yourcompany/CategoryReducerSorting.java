package com.yourcompany;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CategoryReducerSorting extends Reducer<Text, QuantityPriceWritable, Text, Text> {
    public void reduce(ComparableKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        context.write(new Text(key.toString()), new Text(values.iterator().next().toString()));
    }
}
