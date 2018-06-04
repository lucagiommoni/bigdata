package it.polito.bigdata.spark.ex31;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;

public class MyDriver {
	public static void main(String args[]) {
		String inputPath = args[0];
		String outputPath = args[1];
		
		SparkConf conf = new SparkConf().setAppName("Spark ex31");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> inputText = sc.textFile(inputPath);
		JavaRDD<String> ip = inputText.map(line -> line.split("\\s+")[0]);
		ip = ip.distinct();
		ip.saveAsTextFile(outputPath);
		sc.close();
	}
}
