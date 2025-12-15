package com.yourcompany;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;

public class CSVMapper extends Mapper<LongWritable, Text, Text, QuantityPriceWritable> {

    private CSVParser csvParser = new CSVParserBuilder()
            .withSeparator(',')
            .withQuoteChar('"')
            .build();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if (key.get() == 0) {
            String lowerLine = line.toLowerCase();
            if (lowerLine.contains("transaction") && 
                lowerLine.contains("product") && 
                lowerLine.contains("category")) {
                return;
            }
        }
        try {
            String[] fields = csvParser.parseLine(line);

            if (fields.length == 5) {
                String category = fields[2];
                Double price = Double.parseDouble(fields[3].trim());
                int quantity = Integer.parseInt(fields[4].trim());
                context.write(new Text(category), new QuantityPriceWritable(quantity, price));
                return;
            }
        } catch (Exception e) {
            System.err.println("Error parsing line: " + line);
            System.err.println(e);
        }

        System.err.println("Skipped line: " + line);
    }
}