package it.polito.bigdata.hadoop.ex10;

import it.polito.bigdata.hadoop.ex10.MyDriver.MY_COUNTERS;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * MyMapper.java
 *
 * @version 1.0
 *
 * Apr 24, 2018
 */
public class MyMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {

	@Override
	protected void map(
			LongWritable key, Text value,
			Mapper<LongWritable, Text, NullWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
		context.getCounter(MY_COUNTERS.TOT_RECORDS).increment(1);
	}
}
