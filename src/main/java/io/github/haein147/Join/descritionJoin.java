package io.github.haein147.Join;

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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.gson.Gson;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;


public class descritionJoin extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new descritionJoin(), args);
    }
    // id
    // { description : description }
	public static class descriptoinTable extends Mapper<LongWritable, Text, Text, Text> {
		private Text outKey = new Text();
		private Text outValue = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	
			String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), "\t");

			outKey.set(parts[0]);
			outValue.set(parts[1]);
			context.write(outKey, new Text("description\t" + outValue));
			
			
		}
	}
	//title	id	score
	public static class scoreTable extends Mapper<LongWritable, Text, Text, Text> {

		private Text outKey = new Text();
		private Text outValue = new Text();
	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), "\t");
			String v = parts[0] + "\t" + parts[2];
			
			outKey.set(parts[1]);//id
			outValue.set(v);//title	score
			context.write(outKey, new Text("score\t" + outValue));

		}
	}

public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {

		@SuppressWarnings("null")
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String title = "";
			String desc = "";
			String score = "";
			for (Text value : values) {
				String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), "\t");
				if(parts[0] != null) {
					if (parts[0].equals("description")) {
						desc = parts[1];
					} else if (parts[0].equals("score")) {
						title = parts[1].replaceAll("_", " ");
						score = parts[2];
					}
				}else {
					return;
				}
			}
			Gson gson = new Gson();
			resultDto jsonData = gson.fromJson(desc, resultDto.class);
			String desFromJson = jsonData.getDescription();
			resultDto dto = new resultDto();
			dto.setTitle(title);
			dto.setScore(score);
			dto.setDescription(desFromJson);
			String jsonHtml = gson.toJson(dto);
			
			JestClientFactory factory = new JestClientFactory();
			factory.setHttpClientConfig(new HttpClientConfig
	                .Builder("http://elastic.pslicore.io:9200")
	                .multiThreaded(true)
	                .build());
			JestClient client = factory.getObject();
			Index index = new Index.Builder(jsonHtml).index("pagerank").type("page").build();
			System.out.println(client.execute(index));

			context.write(new Text(title),new Text(score));
			
			
		}
	}


	@SuppressWarnings("deprecation")

		@Override
	    public int run(String[] args) throws Exception {
	        Configuration conf = getConf();
			Job job = Job.getInstance(conf, "Join to description and score");

		job.setJarByClass(descritionJoin.class);
		job.setUserClassesTakesPrecedence(true);

		// First dataset to Join
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, descriptoinTable.class);

		// Second dataset to Join
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, scoreTable.class);

		job.setReducerClass(ReduceJoinReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;

	}

}
