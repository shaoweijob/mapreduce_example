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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 程序名: 05.join操作
 * 原理: 添加标识位
 * Created by apple on 16/8/24.
 */
public class Join {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJobName("Join");
        job.setJarByClass(Join.class);

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // 这里必须使用决定路径,小表在 "InputResource/join_1" 中
        job.addCacheFile(new Path("/Users/apple/test/join_1/team").toUri());

        FileInputFormat.addInputPath(job,new Path("InputResource/join_2"));

        FileSystem fs = FileSystem.get(conf);
        Path outdir = new Path("Output/Join_output");
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

class JoinMapper extends Mapper<LongWritable,Text,Text,Text>{
    // 2. join 在内存中通过Map存储小表数据
    Map<String,String> playerTeam = new HashMap<String,String>();
    Text keyInfo = new Text();
    Text valueInfo = new Text();

    /**
     * 每个Mapper实例创建出来以后,会且只会执行一次该方法
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 3. 从小表中读取小表文件,使用临时存储
        URI[] files = context.getCacheFiles();
        for(URI u : files){
            Path p = new Path(u);
            FileInputStream fis = new FileInputStream(p.getName());
            InputStreamReader isr = new InputStreamReader(fis);
            BufferedReader br = new BufferedReader(isr);
            try{
                String line = null;
                while((line = br.readLine()) != null){
                    String[] strs = line.split(",");
                    playerTeam.put(strs[0],line.substring(line.indexOf(",") + 1));
                }
            }finally {
                if(br != null) br.close();
            }
        }
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fs = (FileSplit) context.getInputSplit();
        String fileName = fs.getPath().getName();
        String[] infos = value.toString().split(",");
        keyInfo.set(infos[0]);
        String baseTeam = playerTeam.get(infos[0]);
        // 先判断keyInfo在小表数据里面存在不存在
        if (baseTeam == null){
            return;
        }
        String oldValue = value.toString().substring(value.toString().indexOf(",") +1 );
        String newValue = new String();
        // 4. Reduce join添加标记
        if ("playerNum".equals(fileName)){
            newValue = "0," + oldValue;
        }else if ("playerInfo".equals(fileName)){
            newValue = "1," + oldValue;
        }else if ("playerSal".equals(fileName)){
            newValue = "2," + oldValue;
        }

        // 拼接小表数据
        valueInfo.set(baseTeam + ":" + newValue);
        context.write(keyInfo,valueInfo);

    }
}

class JoinReducer extends Reducer<Text,Text,Text,Text>{
    Text valueInfo = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // 先构造集合,盛装不同标记的value值
        List<String> playerNums = new ArrayList<String>();
        List<String> playerInfos = new ArrayList<String>();
        List<String> playersals = new ArrayList<String>();

//        System.out.println("Key:" + key.toString());
//        for(Text value: values){
//            System.out.println(value.toString());
//        }
//        System.out.println("======================");

        String team = new String();
        for(Text value: values){
            String[] strs = value.toString().split(":");
            team = strs[0];
            String otherInfo = strs[1];
            if (otherInfo.startsWith("0")){
                playerNums.add(otherInfo.substring(2));

            }else if (otherInfo.startsWith("1")){
                playerInfos.add(otherInfo.substring(2));

            }else if (otherInfo.startsWith("2")){
                playersals.add(otherInfo.substring(2));

            }

        }
        if (playerNums.isEmpty()) playerNums.add("");
        if (playerInfos.isEmpty()) playerInfos.add("");
        if (playersals.isEmpty()) playersals.add("");
        for (String str1: playerNums){
            for (String str2: playerInfos){
                for (String str3: playersals){
                    valueInfo.set(team + "," + str1 +"," + str2 + "," +str3);
                    context.write(key,valueInfo);
                }
            }
        }
    }
}