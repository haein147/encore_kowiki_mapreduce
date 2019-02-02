package io.github.haein147.redirectRemove;

import java.util.ArrayList;

public class test {

	public static void main(String[] args) {
		
		ArrayList<String> linkId = new ArrayList<String>();
		ArrayList<String> redirectId = new ArrayList<String>();
		
	    linkId.add("1234");
	    linkId.add("5667");
	    linkId.add("67834");
	    
	    redirectId.add("1234");
	    redirectId.add("5637");
	    
	    if(linkId.removeAll(redirectId)) {
	    	for (String link : linkId) {
				System.out.println(link);			}
		}else {
			System.out.println("gg");
		}
	}
	}

