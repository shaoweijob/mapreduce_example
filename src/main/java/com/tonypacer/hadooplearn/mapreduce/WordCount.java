package com.tonypacer.hadooplearn.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 程序名: 01.词频计算
 * 作用: 计算每个单词在文档中出现次数
 * 注意: 本程序运行在单机环境中,不需要启动集群
 *
 * Created by apple on 16/8/23.
 */
public class WordCount{
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJobName("WC");
        job.setJarByClass(WordCount.class);

        //  在求和计算模型中,可以设置combiner,从而削减Mapper的输出从而减少网络带宽和Reducer的负载
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job,new Path("InputResource/wordCount"));

        FileSystem fs = FileSystem.get(conf);
        Path outdir = new Path("Output/WordCount_output");
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

class WCMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
    // 优化点01: 提前创建keyInfo,valueInfo,以免大量重复创建匿名对象,节省空间和性能开销
    Text keyInfo = new Text();
    IntWritable valueInfo = new IntWritable();
    // 优化点02: 将文档中出现的单词放入 HashMap 中,
    //          遍历数据集元素时,与HashMap 做比对,若HashMap中有该单词,则进行累加;没有该单词,则写入该单词,并计数为1
    //          (注意: 若一行比较短,则没有必要做该优化)
    Map<String,Integer> map = new HashMap<String,Integer>();
    int temp;
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // System.out.println(key.toString() + "     " + value.toString());
        // 使用以上语句可打印出 <key ,value>,
        // 能够清晰地看到,key 为文件的offset ,value 为每一行的数据
        String[] words = value.toString().split(" ");
        for(String word : words){
            if (map.containsKey(word)){
                temp = map.get(word) + 1;
                map.put(word,temp);
            }else{
                map.put(word,1);
            }
            keyInfo.set(word);
            valueInfo.set(map.get(word));
            context.write(keyInfo,valueInfo);
        }
    }
}

class WCReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
    // 优化点03: 作用同优化点01
    Text keyInfo = new Text();
    IntWritable valueInfo = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // System.out.println(key.toString() + "     " + values.toString());
        int sum = 0;
        for(IntWritable value : values){
            sum += value.get();
        }
        keyInfo.set(key);
        valueInfo.set(sum);
        context.write(keyInfo,valueInfo);
    }
}