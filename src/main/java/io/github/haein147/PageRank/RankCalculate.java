package io.github.haein147.PageRank;

import java.io.IOException;

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

public class RankCalculate extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new RankCalculate(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "RankCalculate ");

		job.setJarByClass(RankCalculate.class);
		job.setJobName("RankCalculate");
		
		job.setMapperClass(RankCalculateMapper.class);
		job.setReducerClass(RankCalculateReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setNumReduceTasks(2);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}
	// 504271  1.0	438797,13134,857315,95504,11202
	public static class RankCalculateMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int pageTabIndex = value.find("\t");
	        int rankTabIndex = value.find("\t", pageTabIndex+1);
	  
	        String[] pageValue = StringUtils.splitPreserveAllTokens(value.toString(), "\t");

	        String page = pageValue[0];
	        String rank = pageValue[1];
	        context.write(new Text(page), new Text("!"));
	        
	        if(rankTabIndex == -1) return;

	        if(pageValue[2] != null) {
	        	String[] allOtherPages = StringUtils.splitPreserveAllTokens(pageValue[2], ",");
	        	int totalLinks = allOtherPages.length;
			    for (String otherPage : allOtherPages){
			        String pageRankTotalLinks = rank + "\t" +totalLinks; //1.0 + rank갯수
			        context.write(new Text(otherPage), new Text(pageRankTotalLinks));
			            
		        }
	        }else {
	        	return;
	        }
		        context.write(new Text(page), new Text("|" + pageValue[2]));

		}
	}

	public static class RankCalculateReduce extends Reducer<Text, Text, Text, Text> {

		private static final float damping = 0.85F;
		private static final float N = 1400390;

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			boolean isExistingWikiPage = false;
	        String[] split;
	        float sumShareOtherPageRanks = 0;
	        String links = "";
	        String pageWithRank;
	        
			for (Text value : values) {
				pageWithRank = value.toString();
	            
	            if(pageWithRank.equals("!")) {
	                isExistingWikiPage = true;
	                continue;
	            }
	            if(pageWithRank.startsWith("|")){
	                links = "\t" + pageWithRank.substring(1);
	                continue;
	            }
	            split = pageWithRank.split("\t");
	            float pageRank = Float.valueOf(split[0]);
	            int countOutLinks = Integer.valueOf(split[1]);
	            sumShareOtherPageRanks += (pageRank / countOutLinks);
			}
			if(!isExistingWikiPage) return;
	        float newRank = damping * sumShareOtherPageRanks + (1-damping);
	        context.write(key, new Text(newRank + links));
		}
	}


}
