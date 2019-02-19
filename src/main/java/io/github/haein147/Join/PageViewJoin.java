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

public class PageViewJoin extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new PageViewJoin(), args);
	}


	public static class pageviewMapper extends Mapper<LongWritable, Text, Text, Text> {
		//title	count
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), "\t");
			String title = parts[0].replaceAll("_", " ");
			int count = Integer.parseInt(parts[1]);
			
			context.write(new Text(title), new Text("pv"+"\t"+ String.valueOf(count)));
		}
	}

	public static class swebleMapper extends Mapper<LongWritable, Text, Text, Text> {
		//id {title : title, description : description}
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), "\t");
			String id = parts[0];
			Gson gson = new Gson();
			pvDTO json = gson.fromJson(parts[1], pvDTO.class);
			String title = json.getTitle();
			String des = json.getDescription();
			
			pvDTO dto = new pvDTO();
			dto.setId(id);
			dto.setDescription(des);
			String jsonData = gson.toJson(dto);

			context.write(new Text(title), new Text("sweble"+"\t"+ jsonData));
		}
	}
	public static class editMapper extends Mapper<LongWritable, Text, Text, Text> {
		//title	count
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), "\t");
			String title = parts[0];
			String count = parts[1];
			
			context.write(new Text(title), new Text("edit"+"\t"+ count));
		}
	}
	public static class pageviewReducer extends Reducer<Text, Text, Text, Text> {
		//title (sweble {id : id, description : description }) , (pv ,	count), (edit, count)
		@SuppressWarnings("unused")
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String count = "";
			String edit = "";

			String des = "";
			String id = "";
			String title = key.toString();
			String tmp = "";
			for (Text value : values) {
				String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), "\t");
				if (parts[0].equals("pv")) {
					count = parts[1];
				} else if (parts[0].equals("sweble")) {
					tmp = parts[1];
				} else if (parts[0].equals("edit")) {
					edit = parts[1];
				}else {
					return;
				}
			}
			if(tmp == null) {
				return;
			}else {
				if(count == null) {
					count = "0";
				}
				if (edit == null) {
					edit = "0";
				}
			}
		
			try {
				Gson gson = new Gson();
				pvDTO json = gson.fromJson(tmp, pvDTO.class);
				id = json.getId();
				des = json.getDescription();
				
				pvDTO dto = new pvDTO();
				dto.setTitle(title);
				dto.setPageview(count);
				dto.setDescription(des);
				dto.setEditcount(edit);
				
				String jsonData = gson.toJson(dto);
				context.write(new Text(id), new Text(jsonData));

			}catch(NullPointerException e){
				e.getStackTrace();
			}
			
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "PageViewJoin");

		job.setJarByClass(PageViewJoin.class);

		// First dataset to Join
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, pageviewMapper.class);

		// Second dataset to Join
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, swebleMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, editMapper.class);

		job.setReducerClass(pageviewReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		TextOutputFormat.setOutputPath(job, new Path(args[3]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;

	}

}
