package io.github.haein147.PageRank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RankCalculateReducer extends Reducer<Text, Text, Text, Text> {

	private static final float damping = 0.85F;
	// private static final float N = 1400390;

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		boolean isExistingWikiPage = false;
		String[] split;
		float sumShareOtherPageRanks = 0;
		String links = "";
		String pageWithRank;

		for (Text value : values) {
			pageWithRank = value.toString();

			if (pageWithRank.equals("!")) {
				isExistingWikiPage = true;
				continue;
			}
			if (pageWithRank.startsWith("|")) {
				links = "\t" + pageWithRank.substring(1);
				continue;
			}
			split = pageWithRank.split("\t");

			float pageRank = Float.valueOf(split[0]);
			int countOutLinks = Integer.valueOf(split[1]);

			sumShareOtherPageRanks += (pageRank / countOutLinks);
		}
		if (!isExistingWikiPage)
			return;
		float newRank = damping * sumShareOtherPageRanks + (1 - damping);
		context.write(key, new Text(newRank + links));
	}
}