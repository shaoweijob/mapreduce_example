package com.tonypacer.hadooplearn.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * 09.查找酒店附近地标
 * 
 * 需求：现有100万酒店坐标和20亿地标,里面记录地标的经纬度，请设计mapreduce计算所有酒店1公里范围内的地标。
 * 分析：经纬度以double计算 为 8bit * 2 =16bit，酒店不管是以字符标记，UTF 还是以ID标记 一般不可能超过100bit。
 *      因此，100玩坐标显然为小表数据。
 * 实现思路： 1）将小表在driver中，读入CachFile
 * 			 2）Map阶段，在调用map方法前（可重写setup()/cleanup()方法，都行）,从context中读取缓存的小表数据
 * 			 3）重复调用map方法，将大表中的数据和小表中的数据进行计算，将距离小于1公里的数据，写入context
 * 			 4）聚合，拼接字符串
 * 
 * Created by apple on 16/8/28.
 */
public class NearbyLandmark {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJobName("NearbyLandmark");
        job.setJarByClass(NearbyLandmark.class);

        job.setMapperClass(NLMapper.class);
        job.setReducerClass(NLReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        

        job.addCacheFile(new Path("/Users/apple/Documents/java_workspace/mapreduce_example/InputResource/nearby/hotel/hotel.txt").toUri());

        FileInputFormat.addInputPath(job,new Path("InputResource/nearby/coordinate"));
        FileSystem fs = FileSystem.get(conf);
        Path outdir = new Path("Output/NearbyLandmark");
        if (fs.exists(outdir)) fs.delete(outdir,true);
        FileOutputFormat.setOutputPath(job,outdir);
        if (job.waitForCompletion(true)){
            System.out.println("success!");
        }else {
            System.out.println("failure!");
        }


    }
}

class NLMapper extends Mapper<LongWritable,Text,Text,Text>{
	
    public NLMapper() {
    	super();
//    	System.out.println("生成 mapper");
	}

	Map<String,double[]> hotelMap = new HashMap<String,double[]>();
    final double FIX_DISTANCE = 1000;
    Text valueInfo = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] urls = context.getCacheFiles();
        String line = null;
        for (URI url: urls){
            Path p = new Path(url);
            FileInputStream fis = new FileInputStream(p.getName());
            InputStreamReader isr = new InputStreamReader(fis);
            BufferedReader br = new BufferedReader(isr);
            while((line = br.readLine()) != null){
                String[] strs = line.split(" ");
                String hotelName =strs[0];
                double hotelX = Double.parseDouble(strs[1]);
                double hotelY = Double.parseDouble(strs[2]);
                double[] hotelXY = {hotelX,hotelY};
//                System.out.println("酒店名称："+ hotelName+ ",坐标为："+ hotelXY[0] + "," +hotelXY[1]);
                hotelMap.put(hotelName,hotelXY);
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strs = value.toString().split(" ");
        String coordinateName = strs[0];
        double coordinateX = Double.parseDouble(strs[1]);
        double coordinateY = Double.parseDouble(strs[2]);
        double[] coordinateXY = {coordinateX,coordinateY};
        for (Map.Entry<String,double[]> hotelInfo: hotelMap.entrySet()){
            String hotelName = hotelInfo.getKey();
//            double hotelX = hotelInfo.getValue()[1];
//            double hotelY = hotelInfo.getValue()[2];
            double[] hotelXY = hotelInfo.getValue();
            double d = this.distance(hotelXY,coordinateXY);
//            System.out.println("酒店名称："+ hotelName+ ",坐标为："+ hotelXY[0] + "," +hotelXY[1]);

            if(d <= FIX_DISTANCE){
                valueInfo.set(coordinateName + "," + d);
                System.out.println("coordinateName:"+coordinateName+","+"d" +","+d);
                context.write(new Text(hotelName),valueInfo);
            }

        }

    }

    /**
     * 返回距离
     * @param hotelXY
     * @param coordinateXY
     * @return
     */
    protected double distance(double[] hotelXY,double[] coordinateXY){
        double distanceInX = Math.abs(hotelXY[0]-coordinateXY[0]);
        double distanceInY = Math.abs(hotelXY[1]-coordinateXY[1]);
        double distance = Math.sqrt(distanceInX * distanceInX + distanceInY * distanceInY);
        return distance;

    }
}



class NLReducer extends Reducer<Text,Text,Text,Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String valueInfostr = null;
        
        for (Text value: values){
        	String[] strs = value.toString().split(",");
        	String tempStr = strs[0] +  "(" + strs[1] + ")";
            if (valueInfostr == null){
            	valueInfostr = tempStr;
            }else{
            	valueInfostr += "," + tempStr;
            }
        }
        context.write(key,new Text(valueInfostr));
    }
}

