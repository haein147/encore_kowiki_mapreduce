package io.github.haein147.Join;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ReduceJoin {

	/*
	 * input key : 문서의 라인들이 하나하나 들어옴 input value : [from_id , title] output key :
	 * (조인되는 키가 됨) - title 퀵_정렬 replaceAll("_", " "); output value : from_id
	 */
	public static class SqlMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text outKey = new Text();
		private Text outValue = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), "\t");

			try {
				if (parts[1].length() == 0 || parts[0].length() == 0) {
					context.write(new Text("error"), new Text("from_\t"+"error"));
					System.out.println("======== array err :" + value.toString());

				}
				outKey.set(parts[1]);
				outValue.set(parts[0]);
				context.write(outKey, new Text("from_\t" + outValue));
			} catch (ArrayIndexOutOfBoundsException e) {
				System.out.println("======== array err :" + value.toString());
			}
		}
	}

	/*
	 * input key : 문서의 라인들이 하나하나 들어옴 input value : [to_id , title] output key :
	 * (조인되는 키가 됨) - title 퀵 정렬 output value : to_id
	 */
	public static class MetaMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Text outKey = new Text();
		private Text outValue = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// 퀵_정렬 -> 퀵 정렬
			String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), "\t");

			try {
				if (parts[1].length() == 0 || parts[0].length() == 0) {
					context.write(new Text("error"), new Text("to_\t"+"error"));
					System.out.println("======== array err :" + value.toString());

				}
				outKey.set(parts[1]);
				outValue.set(parts[0]);
				context.write(outKey, new Text("to_\t" + outValue));
			} catch (ArrayIndexOutOfBoundsException e) {
				System.out.println("======== array err :" + value.toString());
			}

		}
	}

	public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
		private ArrayList<Text> from_id = new ArrayList<Text>();
		private ArrayList<Text> to_id = new ArrayList<Text>();
		private static final Text EMPTY_TEXT = new Text("");

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			from_id.clear();
			to_id.clear();

			for (Text value : values) {
				String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), "\t");

				if (parts[0].equals("from_")) {
					from_id.add(new Text(parts[1].toString()));
				} else if (parts[0].equals("to_")) {
					to_id.add(new Text(parts[1].toString()));
				}

				System.out.println("# # # # output :" + to_id + "\t" + from_id);
			}
			
			// output join
			if (!from_id.isEmpty()) {
				for (Text from : from_id) {
					if (!to_id.isEmpty()) {
						for (Text to : to_id) {
							context.write(from, to);
						}
					} else {
						context.write(from, EMPTY_TEXT);
					}
				}
			} else {
				for (Text to : to_id) {
					context.write(EMPTY_TEXT, to);
				}
			}

		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "ReduceSideJoin");
		job.setJarByClass(ReduceJoin.class);

		// First dataset to Join
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, SqlMapper.class);

		// Second dataset to Join
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MetaMapper.class);

		job.setReducerClass(ReduceJoinReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
