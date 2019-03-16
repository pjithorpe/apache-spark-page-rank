package spark;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Iterables;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaUtils;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;
import utils.ISO8601;


public class PageRank {
	
	public static void main(String[] args) {
		int numIterations = Integer.parseInt(args[1]);
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("PageRank-v1"));
		sc.hadoopConfiguration().set("textinputformat.record.delimiter","\n\n");
		JavaRDD<String> records = sc.textFile("file:///" + args[0], 1);
		//System.out.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n" + records.first() + "\nXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
		
		long dateInput = 0;
		try {
			dateInput = ISO8601.toTimeMS(args[2]);
		} catch (ParseException e) {
			System.out.println("Invalid date argument");
			System.exit(1);
		}
		final long maxDate = dateInput;
		JavaPairRDD<String, List<String>> links = records
				.flatMapToPair(s -> {
					String title = "";
					String dateStr = "";
					String word = "";
					List<String> words = new ArrayList<String>();
					boolean skipRecord = false;
					List<Tuple2<String,List<String>>> mapResults = new ArrayList<Tuple2<String,List<String>>>();
					
					//System.out.println("\n\n\n\n\nDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDOING FLATMAPTOPAIRRRRRRRRRRRRRRRRRRRRRRRRRRRR\n\n\n\n\n");
					String[] recordLines = s.split("\n");
					//System.out.println("NUMBER OF LINES IN RECORD: " + Integer.toString(recordLines.length));
					for (int i=0; i<recordLines.length; i++) {
						String[] splitString = recordLines[i].split("\\s");
						
						if(splitString[0].equals("REVISION")) {
							
							long date = 0;
							try {
								date = utils.ISO8601.toTimeMS(splitString[4]);
							} catch (ParseException e) {
								e.printStackTrace();
								date = -1;
							}
							
							if(date < maxDate && date > 0) {
								title = splitString[3];
								dateStr = Long.toString(date);
								words.add(dateStr);
							}
							else {
								return mapResults; //if past the max date, ignore
							}
							//System.out.println("\n\n\n\n\nGOT A TITLE: " + title + "\n\n\n\n\n");
						}
						else if(splitString[0].equals("MAIN")) {
							
							
							int ssPos = 1;
							while(ssPos < splitString.length) {
								word = splitString[ssPos];
								if(!word.equals(title)) {
									//System.out.println("ZXZXZXZXZZXZXZXZXZXZZX " + title + " " + word);
									words.add(word);
								}
								ssPos++;
							}
							mapResults.add(new Tuple2<String, List<String>>(title, words));
						}
					}
					return mapResults;
				}).distinct();
		
		
		
		JavaPairRDD<String, List<String>> latestLinks = links
				.reduceByKey((dAndLs1, dAndLs2) -> {
					if(Long.parseLong(dAndLs1.get(0)) > Long.parseLong(dAndLs2.get(0))) {
						return dAndLs1;
					}
					else {
						return dAndLs2;
					}
				})
				.mapValues(dAndLs -> {
					dAndLs.remove(0);
					return dAndLs;
				})
				.cache();
		
		
		
		//links.saveAsTextFile("file:////users/level4/2130613t/OUTPUT_LINKS");
		
		JavaPairRDD<String, Double> ranks = latestLinks.mapValues(s -> 1.0);
		
		for (int current = 0; current < numIterations; current++) {
			JavaPairRDD<String, Double> contribs = latestLinks.join(ranks).values()
					.flatMapToPair(v -> {
						List<Tuple2<String, Double>> res = new ArrayList<Tuple2<String, Double>>();
						int urlCount = Iterables.size(v._1);
						for (String s : v._1)
							res.add(new Tuple2<String, Double>(s, v._2() / urlCount));
						return res;
					});
			ranks = contribs.reduceByKey((a, b) -> a + b).mapValues(v -> 0.15 + v * 0.85);
		}
		
		//System.out.println("PRINT TEXT FILE");
		ranks.map(result -> result._1 + " " + Double.toString(result._2)).saveAsTextFile("file:////users/level4/2130613t/OUTPUT_RANKS");
		List<Tuple2<String, Double>> output = ranks.collect();
		
		sc.close();
		
	}
}
