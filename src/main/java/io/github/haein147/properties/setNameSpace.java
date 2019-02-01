package io.github.haein147.properties;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

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


//ns.properties 과 pagelinks.tsv / redirect.tsv를 맵사이드 조인하는 클래스
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
        Map<String, String> nsAndTitle = new HashMap<>();
        private Text outKey = new Text();
		private Text outValue = new Text();
		@Override
		public void setup(Context context) throws IOException, InterruptedException{
			File f = new File("ns");
	        FileInputStream fis = new FileInputStream(f);
            BufferedReader in = new BufferedReader(new InputStreamReader(fis));
            
            for (String line : IOUtils.readLines(in)) {
            	//-1=특수
            	//0=\s
            	String[] split = line.split("=");
                String ns = split[0];
                String name = split[1];
                nsAndTitle.put(ns, name);
			}
		}
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), "\t");

			//123	환영합니다	11	-> 123	틀토론:환영합니다
			//142	환영합니다	2	-> 143	사용자:환영합니다 
			String id = parts[0];
			String title = parts[1];
			String namespace = parts[2];
			System.out.printf("##### : %s, %s, %s",id,title,namespace);
			String ns = nsAndTitle.get(namespace);
			if(ns.equals(" ")){
				outKey.set(id);
				outValue.set(title);
			}else {
				//사용자:환영합니다
				outKey.set(id);
				outValue.set(ns + ":" + title);
			}
			context.write(outKey, outValue);
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
