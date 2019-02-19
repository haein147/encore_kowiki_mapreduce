package io.github.haein147.properties;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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
import org.apache.hadoop.util.ToolRunner;;

//ns.properties 과 pagelinks.tsv / redirect.tsv를 맵사이드 조인하는 클래스
public class setNameSpace extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new setNameSpace(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "pageView count");

		job.setJarByClass(setNameSpace.class);
		job.setJobName("pageView count");
		job.setMapperClass(nsMapper.class);
		job.setReducerClass(nsReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		//job.addCacheFile(new URI("/user/mentee/haein/ns.properties#ns"));
		job.setNumReduceTasks(8);

		job.waitForCompletion(true);
		return 0;
	}

	public static class nsMapper extends Mapper<LongWritable, Text, Text, Text> {
		String fromLink = new String();

		ArrayList<String> nsList = new ArrayList<>();

/*		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			File ns_file = new File("ns");
			FileInputStream fis = new FileInputStream(ns_file);
			BufferedReader nsdata = new BufferedReader(new InputStreamReader(fis));

			for (String line : IOUtils.readLines(nsdata)) {
				String[] split = line.split("=");
				String name = split[1];
				nsList.add(name);
			}
		}*/

		// 분류:타이틀
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// ko title count byte
			String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), " ");
			String title = parts[1];
			String pv = parts[2];
			/*String[] titleArray = title.split(":");
			if (titleArray.length != 0) {
				for (String ns : nsList) {
					if (ns.equals(titleArray[0])) {
						
					}
				}
			}*/
			context.write(new Text(title), new Text(pv));
		}
	}

	public static class nsReducer extends Reducer<Text, Text, Text, Text> {

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// title count
			int count = 0;
			
			for (Text value : values) {
				count += Integer.parseInt(value.toString());
			}
			context.write(key, new Text(String.valueOf(count)));

		}
	}
}