package it.polito.bigdata.hadoop.ex;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * MyCombiner.java
 *
 * @version 1.0
 *
 */
public class MyCombiner extends Reducer<Text, NullWritable, Text, NullWritable> {

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {}

	@Override
	protected void reduce(
			Text key,
			Iterable<NullWritable> values,
			Context context)
			throws IOException, InterruptedException {}
}
