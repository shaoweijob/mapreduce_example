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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 程序名: 07.找共同好友 (可能认识的人)
 * 实现思路: 1)在map 阶段,将数据映射为,<朋友组合,自己>
 *         2)在reduce 阶段, 遍历 朋友组合 的 values,便可以得到 两个人的 所有共同好友名单.
 *
 * Created by apple on 16/8/25.
 */
public class FindFriend {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJobName("FindFriend");
        job.setJarByClass(FindFriend.class);

        job.setMapperClass(FFMapper.class);
        job.setReducerClass(FFReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job,new Path("InputResource/findFriend"));
        FileSystem fs = FileSystem.get(conf);
        Path outdir = new Path("Output/FindFriend_output");
        if (fs.exists(outdir)) fs.delete(outdir,true);
        FileOutputFormat.setOutputPath(job,outdir);
        if (job.waitForCompletion(true)){
            System.out.println("success!");
        }


    }

}
class FFMapper extends Mapper<LongWritable,Text,Text,Text>{
    Text friends = new Text();     // 表示自己的朋友组合
    Text owner = new Text();       // 表示自己
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] strs = value.toString().split(",");
        owner.set(strs[0]);      //存放自己
        List<String> friendList = new ArrayList<String>();
        for (int i =1; i < strs.length; i++){
            friendList.add(strs[i]);
        }
        String[] friendArray = new String[friendList.size()];
        friendArray = friendList.toArray(friendArray);
        for (int i = 0; i < friendArray.length; i++){
            for (int j = i+1; j< friendArray.length; j++){
                friends.set(friendArray[i] + "," + friendArray[j]);
                context.write(friends,owner);
            }
        }


    }
}

class FFReducer extends Reducer<Text,Text,Text,Text>{
    Text valueInfo = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String commonFriends = "";
        for (Text value: values){
            if (commonFriends == ""){
                commonFriends = "共同好友有: " + value.toString();
            }else{
                commonFriends += "," + value.toString();
            }
        }
        valueInfo.set(commonFriends);
        context.write(key,valueInfo);


    }
}
