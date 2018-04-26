package it.polito.bigdata.hadoop.ex5;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import it.polito.bigdata.hadoop.ex5.MyCustomType;

/**
 * MyMapper.java
 *
 * @version 1.0
 *
 * Apr 24, 2018
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, MyCustomType> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, MyCustomType>.Context context)
			throws IOException, InterruptedException {
		String[] words = value.toString().split(",");
		MyCustomType customAvg = new MyCustomType();
		customAvg.setCount(1);
		customAvg.setSum(Float.parseFloat(words[2]));
		context.write(new Text(words[0]), customAvg);
	}
}
