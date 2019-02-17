package io.github.haein147.redirectRemove;

import java.io.IOException;
import java.util.ArrayList;

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


/* 1. redirect.sql 로 xml 파싱한 문서를 정제
 * 2. .tsv와 redirect.sql문서를 join해서  from_id가 같으면 redirect되는 문서라고 확인
 * 	meta_id 와 redirect_id 로 tag 부여 , 같으면 tsv가 정제
 * 
 * .tsv from_id to_title
 * redirect.sql의 from_id to_title
 * 
 * 
 * 
 * Reduce input records=2567949
   Reduce output records=1400390

 * */
public class redirectRemoveXml {
	//id	{des : des}
   public static class XmlMapper extends Mapper<Object, Text, Text, Text> {
      
      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), "\t");
         String title = parts[1];
         String id = parts[0];
         context.write(new Text(id), new Text("Xml\t" + title));
      }
   }
   //
   public static class ReMapper extends Mapper<Object, Text, Text, Text> {
         //id	title
         public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), "\t");
            String title = parts[1];
            String id = parts[0];
            context.write(new Text(id), new Text("Redirect\t" + title));
         }
   }
   //id	title
   public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
   
         public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
 			ArrayList<String> title = new ArrayList<String>();
            String retitle = null;
               for (Text value : values) {
                  String parts[] = StringUtils.splitPreserveAllTokens(value.toString(), "\t");
                  if (parts[0].equals("Xml")) {
                	  title.add(parts[1]);
                  } else if(parts[0].equals("Redirect")) {
                	  return;
                  }
               }
               for(String t : title) {
                   context.write(key, new Text(t));
               }

               }
         }
  


   @SuppressWarnings("deprecation")
public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      Job job = new Job(conf, "redirect remove from xml");
      job.setJarByClass(redirectRemoveXml.class);
      job.setReducerClass(ReduceJoinReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      job.setNumReduceTasks(5);
      
      MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, XmlMapper.class);
      MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, ReMapper.class);
      Path outputPath = new Path(args[2]);
      
      
      FileOutputFormat.setOutputPath(job, outputPath);
      outputPath.getFileSystem(conf).delete(outputPath);
      System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
}