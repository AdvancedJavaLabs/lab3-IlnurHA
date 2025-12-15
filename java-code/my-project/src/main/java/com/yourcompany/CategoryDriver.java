package com.yourcompany;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CategoryDriver 
{
    static int REDUCER_WORKERS = 4; 
    public static void main( String[] args ) throws Exception
    {
        // Paths
        var inputData = new Path(args[0]);
        var outputData = new Path(args[1]);
        var tmpData = new Path("/tmp/temp_data");

        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "Category Value Job");

        FileSystem fs = FileSystem.get(conf);
        fs.delete(tmpData, true);
    
        job.setJarByClass(CategoryDriver.class);
        job.setMapperClass(CSVMapper.class);
        job.setReducerClass(CategoryReducer.class);
        job.setNumReduceTasks(REDUCER_WORKERS);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(QuantityPriceWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job, inputData);
        FileOutputFormat.setOutputPath(job, tmpData);

        var startTime1 = System.currentTimeMillis();
        var result = job.waitForCompletion(true);
        var endTime1 = System.currentTimeMillis();

        if (!result) {
            System.exit(1);
        }

        Configuration secondConf = new Configuration();
        secondConf.set("mapreduce.output.textoutputformat.separator", ",");
        Job sortingJob = Job.getInstance(secondConf, "Sorting Job");

        sortingJob.setJarByClass(CategoryDriver.class);
        sortingJob.setMapperClass(CSVMapperSorting.class);
        sortingJob.setReducerClass(CategoryReducerSorting.class);        
        sortingJob.setNumReduceTasks(REDUCER_WORKERS);

        sortingJob.setSortComparatorClass(SortByValue.class);
        sortingJob.setPartitionerClass(SortingPartitioner.class);
        sortingJob.setGroupingComparatorClass(SortingGrouping.class);

        sortingJob.setMapOutputKeyClass(ComparableKey.class);
        sortingJob.setMapOutputValueClass(IntWritable.class);
        sortingJob.setOutputKeyClass(Text.class);
        sortingJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(sortingJob, tmpData);
        FileOutputFormat.setOutputPath(sortingJob, outputData);

        var startTime2 = System.currentTimeMillis();
        result = sortingJob.waitForCompletion(true);
        var endTime2 = System.currentTimeMillis();
        if (!result) {
            System.exit(1);
        }

        System.out.println("Execution Time: " + ((endTime1 - startTime1) + (endTime2 - startTime2)) + " milliseconds");
    }
}
