package io.github.haein147.Sort;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class resultAndSort extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new resultAndSort(), args);
    }
	@Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
		Job job = Job.getInstance(conf, "Result + Sort");

		job.setJarByClass(resultAndSort.class);
		job.setJobName("Result + Sort");
		job.setMapperClass(SortMapper.class);
		//job.setReducerClass(SortReducer.class);
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

	/* input fir
	 * to_id		rank			from_id
	 * 100     0.22840679      556,1424759,152,14647,2271443,1124185,1963110,618,1207495,86,407391,6244,444187
	 */
	
	public static class SortMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text outKey = new Text();
		private Text outValue = new Text();
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), "\t");
			outKey.set(parts[1]);
			outValue.set(parts[0]);
			context.write(outKey, outValue);
		}
	}


}
