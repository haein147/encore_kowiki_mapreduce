package io.github.haein147.PageRank;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Pagerank extends Configured implements Tool {
	
	 private static NumberFormat nf = new DecimalFormat("00");

	    public static void main(String[] args) throws Exception {
	        System.exit(ToolRunner.run(new Configuration(), new Pagerank(), args));
	    }

	    @Override
	    public int run(String[] args) throws Exception {
	    	boolean isCompleted = true;
	    	
	        String lastResultPath = null;

	        for (int runs = 0; runs < 10; runs++) {
	            String inPath = "/user/mentee/haein/iter" + nf.format(runs);
	            lastResultPath = "/user/mentee/haein/iter" + nf.format(runs + 1);

	            isCompleted = runRankCalculation(inPath, lastResultPath);

	            if (!isCompleted) return 1;
	        }

	        if (!isCompleted) return 1;
	        return 0;
	    }
	    
	    private boolean runRankCalculation(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
	        Configuration conf = new Configuration();

	        Job job = Job.getInstance(conf, "rankCalculator");
	        job.setJarByClass(Pagerank.class);

	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);

	        FileInputFormat.setInputPaths(job, new Path(inputPath));
	        FileOutputFormat.setOutputPath(job, new Path(outputPath));

	        job.setMapperClass(RankCalculateMapper.class);
	        job.setReducerClass(RankCalculateReducer.class);

	        return job.waitForCompletion(true);
	    }

	    
	    

}
