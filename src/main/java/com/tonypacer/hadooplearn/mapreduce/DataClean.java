package com.tonypacer.hadooplearn.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 程序名: 03.数据清洗
 * 作用: 根据正则表达式,过滤掉不符合要求的脏数据
 *
 */
public class DataClean {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJobName("DataClean");
        job.setJarByClass(DataClean.class);

        job.setMapperClass(DCMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job,new Path("InputResource/dataClean"));
        FileSystem fs = FileSystem.get(conf);
        Path outdir = new Path("Output/DataClean_output");
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

class DCMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
    Text keyInfo = new Text();
    NullWritable valueInfo = NullWritable.get();

    // 编写正则表达式,利用正则表达式过滤数据
    String patternStr = "^([0-9.]+)\\s([\\w.-]+)\\s"
            + "([\\w.-]+)\\s(\\[[^\\[\\]]+\\])\\s"
            + "\"((?:[^\"]|\\\")+)\"\\s"
            + "(\\d{3})\\s(\\d+|-)\\s"
            + "\"((?:[^\"]|\\\")+)\"\\s"
            + "\"((?:[^\"]|\\\")+)\"$";
    Pattern p = Pattern.compile(patternStr);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Matcher matcher = p.matcher(value.toString());
        if (matcher.find()){
//            System.out.print("有符合的数据");
            String ip = matcher.group(1);
            String uid = matcher.group(2);
            String time = matcher.group(3);
            String url = matcher.group(8);
            keyInfo.set(ip + "," + uid + "," + time + "," + url);
            context.write(keyInfo,valueInfo);

        }

    }
}