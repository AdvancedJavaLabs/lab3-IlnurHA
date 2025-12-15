package com.yourcompany;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;

public class CSVMapperSorting extends Mapper<LongWritable, Text, ComparableKey, IntWritable> {

    private CSVParser csvParser = new CSVParserBuilder()
            .withSeparator(',')
            .withQuoteChar('"')
            .build();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        try {
            String[] fields = csvParser.parseLine(line);

            if (fields.length == 3) {
                String category = fields[0];
                Double price = Double.parseDouble(fields[1].trim());
                int quantity = Integer.parseInt(fields[2].trim());
                context.write(new ComparableKey(price, category), new IntWritable(quantity));
                return;
            }
        } catch (Exception e) {
            System.err.println("Error parsing line: " + line);
            System.err.println(e);
        }

        System.err.println("Skipped line: " + line);
    }
}