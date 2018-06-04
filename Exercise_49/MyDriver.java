package it.polito.bigdata.spark.ex49;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;

public class MyDriver {

	public static void main(String[] args) {

		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];

		SparkConf conf = new SparkConf().setAppName("Spark ex49");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> inputText = sc.textFile(inputPath);


		sc.close();
	}
}
