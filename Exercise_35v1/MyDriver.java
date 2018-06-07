package it.polito.bigdata.spark.ex35;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;

/**
 * 
 * MyDriver.java
 *
 * @version 1.0
 *
 * Jun 7, 2018
 * 
 * Map Reduce Filter
 */
public class MyDriver {

	public static void main(String[] args) {

		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];

		SparkConf conf = new SparkConf().setAppName("Spark ex35");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> inputText = sc.textFile(inputPath);
		Double max = inputText.map(line->new Double(line.split(",")[2])).reduce((val1,val2)->{return val1 > val2 ? val1 : val2;});
		
		inputText.filter(line->Double.parseDouble(line.split(",")[2]) == max).map(line->line.split(",")[1]).distinct().saveAsTextFile(outputPath);
		
		sc.close();
	}
}
	