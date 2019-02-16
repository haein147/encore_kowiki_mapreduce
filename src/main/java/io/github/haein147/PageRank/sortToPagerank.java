package io.github.haein147.PageRank;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class sortToPagerank {

   public static class MetaMapper extends Mapper<Object, Text, Text, Text> {

      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), "\t");
         String from_id = parts[0];
         String to_id = parts[1];
         context.write(new Text(from_id), new Text(to_id));
      }
   }

   public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {

      public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
         String pagerank = "1.0	";

         boolean first = true;
         
         for (Text value : values) {
            if(!first) {
               pagerank += ",";
            }
            pagerank += value.toString();
               first = false;
         }

         context.write(key, new Text(pagerank));
      }
   }

   @SuppressWarnings("deprecation")
public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      Job job = new Job(conf, "xml tsv join");
      job.setJarByClass(sortToPagerank.class);
      job.setMapperClass(MetaMapper.class);
      job.setReducerClass(ReduceJoinReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      job.setNumReduceTasks(20);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
}