package io.github.haein147.redirectRemove;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * XML : 1000001	토론:고개
 * pagerank_sort : rank	to_id
 */
public class rankNameJoin {
   public static class XmlMapper extends Mapper<Object, Text, Text, Text> {
      //1000001	토론:고개
      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), "\t");
         String title = parts[1];
         String id = parts[0];
         context.write(new Text(id), new Text("Xml\t" + title));
      }
   }

   public static class pagerankMapper extends Mapper<Object, Text, Text, Text> {
         //pagerank_sort : rank	to_id
         public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), "\t");
            String rank = parts[0];
            String id = parts[1];
            context.write(new Text(id), new Text("Rank\t" + rank));
         }
   }

   public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
	   /*
	    * XML : 1000001	토론:고개
	    * pagerank_sort : rank	to_id
	    * reduce input : 1000001	[Xml	토론:고개, Rank	rank]...
	    */
         public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String title = null;
            String rank = null;
               for (Text value : values) {
                  String parts[] = StringUtils.splitPreserveAllTokens(value.toString(), "\t");
                  if (parts[0].equals("Xml")) {
                     title = parts[1];
                  } else if(parts[0].equals("Rank")) {
                	  rank = parts[1];
                  }
                  
               } 
               if(title == null || rank == null) {
             	  return;
               }else {
              	 context.write(new Text(title), new Text(key + "\t" + rank));
               }
            }
         }


   @SuppressWarnings("deprecation")
public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      Job job = new Job(conf, "redirect remove from xml");
      job.setJarByClass(rankNameJoin.class);
      job.setReducerClass(ReduceJoinReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      job.setNumReduceTasks(5);
      
      MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, XmlMapper.class);
      MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, pagerankMapper.class);
      Path outputPath = new Path(args[2]);
      
      
      FileOutputFormat.setOutputPath(job, outputPath);
      outputPath.getFileSystem(conf).delete(outputPath);
      System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
}