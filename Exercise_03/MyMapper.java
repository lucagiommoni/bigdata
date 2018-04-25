package it.polito.bigdata.hadoop.ex3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * MyMapper.java
 *
 * @version 1.0
 *
 * Apr 24, 2018
 */
public class MyMapper extends Mapper<Text, Text, Text, IntWritable> {

	private static Double PM10Threshold = new Double(50);

	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		String[] fields = key.toString().split(",");

    if (new Double(value.toString()) > PM10Threshold) {
			context.write(new Text(fields[0]), new IntWritable(1));
		}
	}
}
