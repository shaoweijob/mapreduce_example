package com.tonypacer.hadooplearn.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 程序名: 04.数据去重
 * 作用: 根据需求,去重
 * 原理: 利用 key 只能唯一的特性
 * Created by apple on 16/8/24.
 */
public class Distinct {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJobName("Distinct");
        job.setJarByClass(Distinct.class);

        job.setMapperClass(DistinctMapper.class);
        job.setReducerClass(DistinctReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job,new Path("InputResource/distinct"));

        FileSystem fs = FileSystem.get(conf);
        Path outdir = new Path("Output/Distinct_output");
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

class DistinctMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
    // 将要输出的key和value作为mapper类的属性，避免产生大量的临时对象，对象重用
    Text keyInfo = new Text();
    NullWritable valueInfo = NullWritable.get();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        keyInfo.set(value);
        context.write(keyInfo,valueInfo);
    }
}

class DistinctReducer extends Reducer<Text,NullWritable,Text,NullWritable>{
    Text keyInfo = new Text();
    NullWritable valueInfo = NullWritable.get();
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        keyInfo.set(key);
        context.write(keyInfo,valueInfo);
    }
}