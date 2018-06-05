package it.polito.bigdata.spark.ex33;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 
 * MyDriver.java
 *
 * @version 1.0
 *
 * Jun 5, 2018
 * 
 * Extract top 3 using map and top methods of JavaRDD
 */
public class MyDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Exercise #33 v1"));
		
		for(Double val : sc.textFile(inputPath).map(line->Double.parseDouble(line.split(",")[2])).top(3)) {
			System.out.println(val);
		}
		
		sc.close();
	}
}
