package io.github.haein147.counter;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import io.github.haein147.properties.setNameSpace;

public class linksCounter extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new linksCounter(), args);
    }
	@Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
		Job job = Job.getInstance(conf, "__ 1");

		job.setJarByClass(linksCounter.class);
		job.setJobName("Join");
		job.setMapperClass(linksMapper.class);
		job.setReducerClass(linksReducer.class);
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

	/*
	 * meta.xml을 파싱하는 클래스
	 * <page>단위로 잘라서 하나의 value가 되어서 doc.select로 뽑아낸다.
	 * Map input records=1984152
       Map output records=1984151
       Reduce input records=1984151
       Reduce output records=1984151
	
		=> error난 recode : 1개
		output : key - to_id / value - from_title
	 * */
	
	public static class linksMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = new String(value.toString());
			String xml = line.replaceAll("^.*<page>", "<page>") + "</page>";
			Document doc = Jsoup.parse(xml);

			String id = doc.select("page > id").text();
			String title = doc.select("page > title").text().replaceAll(" ", "_");
			
			if (!id.isEmpty() && !title.isEmpty()) {
				context.write(new Text(id), new Text(title));
			}else {
				System.out.printf("========error======= : ", doc);
			}
		}
	}

	public static class linksReducer extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// 퀵소트 퀵 정렬 0
			// 퀵 정렬 25
			for (Text value : values) {
				context.write(key, value);
			}
		}
	}
}