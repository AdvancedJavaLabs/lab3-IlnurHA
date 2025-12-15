package com.yourcompany;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CategoryReducer extends Reducer<Text, QuantityPriceWritable, Text, Text> {
    public void reduce(Text key, Iterable<QuantityPriceWritable> values, Context context) throws IOException, InterruptedException {
        var myQuantityPriceWritable = new QuantityPriceWritable(0, 0); 
        for (QuantityPriceWritable value: values) {
            myQuantityPriceWritable.merge(value);
        } 
        context.write(key, new Text(myQuantityPriceWritable.toString()));
    }
}
