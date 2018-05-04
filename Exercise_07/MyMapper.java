package it.polito.bigdata.hadoop.ex7;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * MyMapper.java
 *
 * @version 1.0
 *
 * Apr 24, 2018
 */
public class MyMapper extends Mapper<Text, Text, Text, Text> {

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
	}

	@Override
	protected void map(
			Text key,
			Text value,
			Context context)
			throws IOException, InterruptedException {
		
		String[] words = value.toString().split("\\s+");
		for (String word : words) {
			if (word.compareToIgnoreCase("not") != 0 &&
				word.compareToIgnoreCase("and") != 0 &&
				word.compareToIgnoreCase("or")  != 0) {				
				context.write(new Text(word.toLowerCase()), key);
			}
		}
		
	}

}
