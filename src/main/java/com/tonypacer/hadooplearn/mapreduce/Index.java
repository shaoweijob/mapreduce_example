package com.tonypacer.hadooplearn.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 程序名: 06.倒排索引
 * 作用: 定位数据在文档中的位置
 *
 * Created by apple on 16/8/24.
 */
public class Index {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJobName("Index");
        job.setJarByClass(Index.class);

        job.setMapperClass(IndexMapper.class);
        // 设置分区类
        job.setPartitionerClass(MyPartitioner.class);
        // 设置reduce task 数量
        job.setNumReduceTasks(3);
        job.setReducerClass(IndexReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job,new Path("InputResource/index"));

        FileSystem fs = FileSystem.get(conf);
        Path outdir = new Path("Output/Index_output");
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

class IndexMapper extends Mapper<LongWritable,Text,Text,Text>{
    Text keyInfo = new Text();
    Text valueInfo = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");
        Map<String,Integer> counts = new HashMap<String,Integer>();
        Map<String,String> locations = new HashMap<String,String>();
        int i = 0;
        for(String word: words){
            i ++;
            if (counts.containsKey(word)){
                counts.put(word,counts.get(word) + 1);
            }else{
                counts.put(word,1);
            }

            if (locations.containsKey(word)){
                locations.put(word,locations.get(word) + ":" + i);
            }else{
                locations.put(word,i + "");
            }
        }

        for(Map.Entry<String,Integer> entry: counts.entrySet()){
            keyInfo.set(entry.getKey());
            valueInfo.set(key.toString() + "," + entry.getValue() + "," + locations.get(entry.getKey()));
            context.write(keyInfo,valueInfo);
        }
    }
}

class MyPartitioner extends HashPartitioner<Text,Text>{
    /**
     * 返回值 在 0 ~ numReduceTask-1
     */
    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
        if (key.toString().startsWith("h")) return 0;
        if (key.toString().startsWith("s")) return 1;
        if (key.toString().startsWith("m")) return 2;
        return 3;

    }
}

class IndexReducer extends Reducer<Text,Text,Text,Text>{
    Text valueInfo = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder("");
        boolean isFirst = true;
        for (Text value: values){
            if(isFirst){
                sb.append(value.toString());
                isFirst = false;
            }else{
                sb.append(";" + value.toString());
            }

        }
        valueInfo.set(sb.toString());
        context.write(key,valueInfo);
    }
}