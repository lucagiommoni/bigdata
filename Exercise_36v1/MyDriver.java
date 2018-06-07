package it.polito.bigdata.spark.ex36;

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
 * map reduce
 */
public class MyDriver {

	public static void main(String[] args) {

		String inputPath = args[0];

		SparkConf conf = new SparkConf().setAppName("Spark ex36");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> inputText = sc.textFile(inputPath);
	
		System.out.println(
			inputText.map(line->Double.parseDouble(line.split(",")[2])).map(el->new Avg(el, 1)).reduce((el1,el2)->{
					el1.setCount(el1.getCount()+1);
					el1.setSum(el1.getSum()+el2.getSum());
					return el1;
				}
			).computeAvg()
		);

		sc.close();
	}
}
