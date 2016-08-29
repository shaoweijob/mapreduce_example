package com.tonypacer.hadooplearn.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * 程序名: 08.求平均数
 * Created by apple on 16/8/25.
 */
public class Average {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJobName("Average");
        job.setJarByClass(Average.class);

        job.setMapperClass(AverageMapper.class);
        job.setReducerClass(AverageReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job,new Path("InputResource/average"));

        FileSystem fs = FileSystem.get(conf);
        Path outdir = new Path("Output/Average_output");
        if (fs.exists(outdir)){
            fs.delete(outdir,true);
        }
        FileOutputFormat.setOutputPath(job,outdir);
        boolean success = job.waitForCompletion(true);
        if (success) {
            System.out.print("success!");
        }
    }
}

class AverageMapper extends Mapper<LongWritable,Text,Text,DoubleWritable>{
    Text keyInfo = new Text();
    DoubleWritable valueInfo = new DoubleWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strs = value.toString().split(" ");
        keyInfo.set(strs[0]);
        valueInfo.set(Double.parseDouble(strs[1]));
        context.write(keyInfo,valueInfo);
    }
}

class AverageReducer extends Reducer<Text,DoubleWritable,Text,Text>{
    Text valueInfo = new Text();
    double average;
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        int count = 0;
        for (DoubleWritable value: values){
            sum += value.get();
            count ++;

        }
        average = sum / count;
        DecimalFormat df = new DecimalFormat("#.00");
        valueInfo.set(df.format(average));
        context.write(key,valueInfo);
    }
}



