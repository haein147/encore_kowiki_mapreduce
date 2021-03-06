package io.github.haein147.parsing;

import java.io.IOException;

import org.apache.commons.lang3.StringEscapeUtils;
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
import org.jsoup.nodes.TextNode;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;
import org.sweble.wikitext.engine.EngineException;
import org.sweble.wikitext.engine.PageId;
import org.sweble.wikitext.engine.PageTitle;
import org.sweble.wikitext.engine.WtEngineImpl;
import org.sweble.wikitext.engine.config.WikiConfig;
import org.sweble.wikitext.engine.nodes.EngProcessedPage;
import org.sweble.wikitext.engine.output.HtmlRenderer;
import org.sweble.wikitext.engine.output.HtmlRendererCallback;
import org.sweble.wikitext.engine.output.MediaInfo;
import org.sweble.wikitext.engine.utils.DefaultConfigEnWp;
import org.sweble.wikitext.engine.utils.UrlEncoding;
import org.sweble.wikitext.parser.nodes.WtUrl;
import org.sweble.wikitext.parser.parser.LinkTargetException;

import com.google.gson.Gson;
import com.sun.xml.bind.v2.model.core.Element;



public class App extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new App(), args);
    }
	@Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("textinputformat.record.delimiter", "</page>");
		Job job = Job.getInstance(conf, "xmlParser");

		job.setJarByClass(App.class);
		job.setJobName("xmlParser ");
		job.setMapperClass(linksMapper.class);
		job.setReducerClass(linksReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setNumReduceTasks(100);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}
/*
 * id	title	plantext
 * sweble parser로 wikitext를 파싱 후
 * <page></page>단위로 delimiter처리 후 한페이지씩 파싱작업
 * */
	
	public static class linksMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		public static String convertWikiText(String title, String wikiText, boolean renderHtml) throws LinkTargetException, EngineException, IOException {

			// Set-up a simple wiki configuration
		    WikiConfig config = DefaultConfigEnWp.generate();
			final int wrapCol = wikiText.length();

		    // Instantiate a compiler for wiki pages
		    WtEngineImpl engine = new WtEngineImpl(config);
		    // Retrieve a page
		    PageTitle pageTitle = PageTitle.make(config, title);
		    PageId pageId = new PageId(pageTitle, -1);
		    // Compile the retrieved page
		    EngProcessedPage cp = engine.postprocess(pageId, wikiText, null);

			TextConverter p = new TextConverter(config, wrapCol);
			return (String) p.go(cp.getPage());
		}
	
	

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = new String(value.toString());
			String xml = line.replaceAll("^.*<page>", "<page>") + "</page>";
			boolean re = false;
			
			Document wikitext = Jsoup.parse(xml, "", Parser.xmlParser());
			
			String title = wikitext.select("page > title").text();
			String id = wikitext.select("page > id").text();
			String ns = wikitext.select("page > ns").text();
			
			wikitext.outputSettings(new Document.OutputSettings().prettyPrint(false));
			String wiki = "";
			String html = null;
			
			if (ns.equals("0")) {
				try {
					for(TextNode node : wikitext.select("text").get(0).textNodes()){
					    wiki = wiki + node + "\n\n";
					}
					wiki = StringEscapeUtils.unescapeXml(wiki);
					html = convertWikiText(title, wiki, false);
					html = html.replaceAll("\\[\\[파일:.*\\]\\]", "");
					Gson gson = new Gson();
					tableDto dto = new tableDto();
					dto.setDescription(html);
					dto.setTitle(title);
					String jsonHtml = gson.toJson(dto);
					
					context.write(new Text(id), new Text(jsonHtml));

				}catch(IndexOutOfBoundsException e) {
					System.out.println("### IndexOutOfBoundsException : " + title);
				}catch (LinkTargetException e) {
					e.printStackTrace();
				}catch (EngineException e) {
					e.printStackTrace();
				}catch (NullPointerException e) {
					e.printStackTrace();
				}
			}else {
				return;
			}

		}
	}

	public static class linksReducer extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text value : values) {
				context.write(key, value);
			}
		}
	}
}