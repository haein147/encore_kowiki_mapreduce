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
 * */
public class redirectRemoveTsv {
    public static void main(String[] args) throws Exception {
       
        Configuration conf = new Configuration();
	    Job job = new Job(conf, "redirect remove from Tsv");
	    job.setJarByClass(redirectRemoveTsv.class);
	    job.setReducerClass(ReduceJoinReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setNumReduceTasks(5);
	    
	    MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, PagelinkMapper.class);
	    MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, ReMapper.class);
	    Path outputPath = new Path(args[2]);
	    
	    
	    FileOutputFormat.setOutputPath(job, outputPath);
	    outputPath.getFileSystem(conf).delete(outputPath);
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	 }
    
    
   public static class PagelinkMapper extends Mapper<Object, Text, Text, Text> {
	  Text outKey = new Text();
	  Text outValue = new Text();

	   //89589   퀵_정렬 
      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), "\t");
         outKey.set(parts[1]);	//title
         outValue.set(parts[0]);	//id
         context.write(outKey, new Text("Pagelink\t" + outValue));
      }
   }

   public static class ReMapper extends Mapper<Object, Text, Text, Text> {
	   	Text outKey = new Text();
		Text outValue = new Text();
         //89589   퀵_정렬 	0
         public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = StringUtils.splitPreserveAllTokens(value.toString(), "\t");
            outKey.set(parts[1]);	//title
            outValue.set(parts[0]);	//id
            context.write(outKey, new Text("Redirect\t" + outValue));
         }
   }
   //Redirect        506043  퀵_정렬
   //Pagelink        760797  퀵_정렬
   public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
	   ArrayList<String> linkId = new ArrayList<>();
	   ArrayList<String> redirectId = new ArrayList<>();
         public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
               for (Text value : values) {
            	   //id가 서로 같을때 없애준다 = id가 다를때 context.write
                  String parts[] = StringUtils.splitPreserveAllTokens(value.toString(), "\t");
                  if (parts[0].equals("Pagelink")) {
                     linkId.add(parts[1]);	//760797
                  } else if(parts[0].equals("Redirect")) {
                	  redirectId.add(parts[1]); //506043
                  }
                  
               }
               for(String link :linkId) {
            	   for(String re : redirectId) {
            		   if(!link.equals(re)) {
                      	  context.write(new Text(link), key);
            	   }
               }
               }
         }
   }
}