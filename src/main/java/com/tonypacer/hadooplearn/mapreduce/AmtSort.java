package com.tonypacer.hadooplearn.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 程序名: 02.二次排序
 * 作用: 在key排序的基础上,对value也进行排序.
 *
 */
public class AmtSort {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJobName("AmtSort");
        job.setJarByClass(AmtSort.class);

        job.setMapperClass(AmtSortMapper.class);
//        job.setSortComparatorClass(MySorter.class);

        job.setReducerClass(AmtSortReducer.class);
        job.setMapOutputKeyClass(MyKey.class);
        job.setMapOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job,new Path("InputResource/amtSort"));
        FileSystem fs = FileSystem.get(conf);
        Path outdir = new Path("Output/AmtSort_output");
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

/**
 * Hadoop的IO中,自己定义了许多Writable的接口,比如IntWritable,Text
 * 这些类都通过实现WritableComparable接口提供了自己的比较策略
 * 以下,创建了自己定义的序列化类
 *
 * WritableComparable<T> 继承了Writable和 Comparable<T>接口
 * 注意:
 *      除了WritableComparable<T>接口外,还有一个RawComparator
 *      区别:RawComparator是直接比较数据流的数据，不需要数据流反序列化成对象，省去了新建对象的开销
 */
class MyKey implements WritableComparable<MyKey>{
    /** WritableComparable<T> 继承了Writable和 Comparable<T>接口
    *
     * 因此implements WritableComparable</T>的话,
     * 需要实现序列化特性 和 比较的特性
    */
    private int acctNo;
    private double amt;

    public int getAcctNo() {
        return acctNo;
    }

    public void setAcctNo(int acctNo) {
        this.acctNo = acctNo;
    }

    public double getAmt() {
        return amt;
    }

    public void setAmt(double amt) {
        this.amt = amt;
    }

    /**
     * 比较两个key值是否相同
     */
    @Override
    public int compareTo(MyKey other) {
        int result = Integer.compare(this.acctNo,other.getAcctNo());
        if (result == 0){
            return -Double.compare(this.amt,other.getAmt());
        }
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(acctNo);
        out.writeDouble(amt);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.acctNo = in.readInt();
        this.amt = in.readDouble();
    }
}

class AmtSortMapper extends Mapper<LongWritable,Text,MyKey,NullWritable>{
    MyKey keyInfo = new MyKey();
    NullWritable valueInfo = NullWritable.get();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] amtAcct = value.toString().split(",");
        int acctNo = Integer.parseInt(amtAcct[0]);
        double amt = Double.parseDouble(amtAcct[1]);
        keyInfo.setAcctNo(acctNo);
        keyInfo.setAmt(amt);
        context.write(keyInfo,valueInfo);
    }
}

class AmtSortReducer extends Reducer<MyKey,NullWritable,IntWritable,DoubleWritable>{
    IntWritable keyInfo = new IntWritable();
    DoubleWritable valueInfo = new DoubleWritable();
    @Override
    protected void reduce(MyKey key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        keyInfo.set(key.getAcctNo());
        valueInfo.set(key.getAmt());
        context.write(keyInfo,valueInfo);
    }
}

//class MySorter extends WritableComparator{
//    public MySorter() {
//        // 告诉这个排序类,key类型为 Mykey,第二个参数一定要为true
//        super(MyKey.class,true);
//    }
//
//    /**
//     * 重写方法,实现我们自己的逻辑
//     */
//    @Override
//    public int compare(WritableComparable a, WritableComparable b) {
//        // 类型转换
//        MyKey myKeyA = (MyKey) a;
//        MyKey myKeyB = (MyKey) b;
//
//        // 按账号正序排序,按金额倒序排序
//        int result = myKeyA.compareTo(myKeyB);
//        return result;
//
//    }
//}