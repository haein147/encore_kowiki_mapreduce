package io.github.haein147.counter;

import org.apache.commons.lang.StringUtils;
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
import org.jsoup.parser.Parser;

import java.io.IOException;

public class xmlParsing extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new xmlParsing(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("textinputformat.record.delimiter", "</page>");
        Job job = new Job(conf, "xml parsing");
        job.setJarByClass(this.getClass());
        job.setMapperClass(ExtractorMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(BypassReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(5);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        return 0;
    }

    public static class ExtractorMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(value.toString(), "<page>");
            String contents;
            try {
                if (split.length == 1) {
                    contents = split[0];
                } else {
                    contents = split[1];
                }
            } catch (ArrayIndexOutOfBoundsException e) {
                System.out.println(value.toString());
                return;
            }
            Document doc = Jsoup.parse("<page>" + contents + "</page>", "", Parser.xmlParser());
            String id = doc.select("page > id").text();
            String nameSpace = doc.select("page > ns").text();
            String title = doc.select("page > title").text();
            if (nameSpace.equals("0")) {
                context.write(new Text(id), new Text(title));
            }
        }
    }

    public static class BypassReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(new Text(key), value);
            }
        }
    }

}