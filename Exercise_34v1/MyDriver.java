package it.polito.bigdata.spark.ex34;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;

/**
 *
 * MyDriver.java
 *
 * @version 1.0
 *
 * Jun 5, 2018
 *
 * Report the line(s) associated with the maximum value of PM10
 * Use map, reduce and filter
 */
public class MyDriver {

	public static void main(String[] args) {

		String inputPath = args[0];
		String outputPath = args[1];

		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Spark ex34"));

		JavaRDD<String> inputText = sc.textFile(inputPath);

		Double max = inputText.map(line->Double.parseDouble(line.split(",")[2])).reduce((el1,el2)->{return el1>el2?el1:el2;});

		inputText.filter(line -> {return Double.parseDouble(line.split(",")[2]) == max;}).saveAsTextFile(outputPath);;

		sc.close();
	}
}
