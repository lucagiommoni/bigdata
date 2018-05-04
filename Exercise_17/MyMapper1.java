package it.polito.bigdata.hadoop.ex17;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * MyMapper.java
 *
 * @version 1.0
 *
 */
public class MyMapper1 extends Mapper<LongWritable, Text, Text, FloatWritable> {
	
	private HashMap<String, Float> map;

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		map = new HashMap<>();
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		for (Entry<String, Float> entry : map.entrySet()) {
			context.write(new Text(entry.getKey()), new FloatWritable(entry.getValue()));
		}
	}

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Context context)
			throws IOException, InterruptedException {
		String[] tmp = value.toString().toLowerCase().split(",");
		if (map.get(tmp[1]) == null) {
			map.put(tmp[1], Float.MIN_VALUE);
		}
		if (map.get(tmp[1]) < Float.parseFloat(tmp[3])) {
			map.put(tmp[1], Float.parseFloat(tmp[3]));
		}
	}

}
