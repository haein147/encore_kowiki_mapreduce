package io.github.haein147.PageRank;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;

public class test {

	public static void main(String[] args) {
		String data ="1747717 1.0     217260,1748121,1653854";

        String[] pageValue = data.toString().split("\\s");

        String page = pageValue[0];
        String rank = pageValue[1];
        System.out.printf("1. key : %s, value: %s" , page, rank);
        System.out.println();

        if(pageValue[2] != null	) {
        	String[] allOtherPages = StringUtils.splitPreserveAllTokens(pageValue[2], ",");
        	int totalLinks = allOtherPages.length;
		    for (String otherPage : allOtherPages){
		        Text pageRankTotalLinks = new Text(rank + totalLinks); //1.0 + rank갯수
		        System.out.printf("2. key : %s, value: %s" , otherPage, pageRankTotalLinks);
		        System.out.println();
    
	        }
        }else {
        	return;
        }
	        System.out.printf("3. key : %s, value: %s" , page, pageValue[2]);
	        System.out.println();

	}
}
