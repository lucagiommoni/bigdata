package it.polito.bigdata.spark.ex50;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;

public class MyDriver {

	public static void main(String[] args) {

		String inputPath;
		String outputPath;

		inputPath = args[0];
		outputPath = args[1];

		SparkConf conf = new SparkConf().setAppName("Spark ex50");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> inputText = sc.textFile(inputPath);


		sc.close();
	}
}
