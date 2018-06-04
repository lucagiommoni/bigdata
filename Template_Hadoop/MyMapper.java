package it.polito.bigdata.hadoop.ex??;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * MyMapper.java
 *
 * @version 1.0
 *
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {}

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Context context)
			throws IOException, InterruptedException {}

}
