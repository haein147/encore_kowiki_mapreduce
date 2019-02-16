package io.github.haein147.PageRank;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RankCalculateMapper extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		int pageTabIndex = value.find("\t");
		int rankTabIndex = value.find("\t", pageTabIndex + 1);

		String[] pageValue = StringUtils.splitPreserveAllTokens(value.toString(), "\t");

		String page = pageValue[0];
		String rank = pageValue[1];
		context.write(new Text(page), new Text("!"));

		if (rankTabIndex == -1) return;

		if (pageValue[2] != null) {
			String[] allOtherPages = StringUtils.splitPreserveAllTokens(pageValue[2], ",");
			int totalLinks = allOtherPages.length;
			
			for (String otherPage : allOtherPages) {
				String pageRankTotalLinks = rank + "\t" + totalLinks; // 1.0 + rank갯수
				context.write(new Text(otherPage), new Text(pageRankTotalLinks));
			}
		} else {
			return;
		}
		context.write(new Text(page), new Text("|" + pageValue[2]));

	}
}