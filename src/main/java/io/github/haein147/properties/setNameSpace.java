package io.github.haein147.properties;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
;

public class setNameSpace extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new setNameSpace(), args);
    }
	@Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
		Job job = Job.getInstance(conf, "pagerank __ 1");


		job.setJarByClass(setNameSpace.class);
		job.setJobName("Page Rank - Example 2");
		job.setMapperClass(nsMapper.class);
		job.setReducerClass(nsReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
        job.addCacheFile(new URI("/user/mentee/haein/ns.properties#ns"));
		job.setNumReduceTasks(8);

		job.waitForCompletion(true);
		return 0;
	}
	public static class nsMapper extends Mapper<LongWritable, Text, Text, Text> {
		String fromLink = new String();
        Map<Integer, String> nsAndTitle = new HashMap<>();

		@Override
		public void setup(Context context) throws IOException, InterruptedException{
			File f = new File("./ns");
            BufferedReader in = new BufferedReader(new FileReader(context.getCacheFiles()[0].toString()));
            for (String line : IOUtils.readLines(in)) {
            	//-1=특수
            	String[] split = line.split("=");
                int id = Integer.parseInt(split[0]);
                String title = split[1];
                nsAndTitle.put(id, title);
			}
		}
		
		@SuppressWarnings("unlikely-arg-type")
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//123	11	환영합니다
			String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), "\t");
			//String fileName = "/ns.properties";
	        //InputStream is = setNameSpace.class.getResourceAsStream(fileName);
	        //BufferedReader reader = new BufferedReader(new InputStreamReader(is));
	        
			
			//Configuration conf  = new Configuration();
	        //String filePath = "/ns.properties";
	        //Path path = new Path(filePath);
	        //FileSystem fs = path.getFileSystem(conf);
			//hdfs://ip-172-26-13-65.ap-northeast-2.compute.internal:8020

	      //123	환영합니다	11
			String id = parts[0];
			String title = parts[1];
			String namespace = parts[2];
			String ns = nsAndTitle.get(namespace);
			if(ns.isEmpty()){
				fromLink = title;
			}else {
				fromLink = ns + ":" + title; //사용자:환영합니다
			}
			
			context.write(new Text(id), new Text(fromLink));
		}
	}

	public static class nsReducer extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			//123	사용자:환영합니다
			for (Text value : values) {
				context.write(key, value);
			}
		}
	}
}
