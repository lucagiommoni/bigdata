package it.polito.bigdata.hadoop.ex28;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * MyMapper.java
 *
 * @version 1.0
 *
 */
public class MyMapper2 extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Context context)
			throws IOException, InterruptedException {
		String[] tmp = value.toString().split(",");
		context.write(new Text(tmp[1]), new Text("answer:" + tmp[0] + "," + tmp[3]));
	}

}
