package it.polito.bigdata.hadoop.ex28;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * MyReducer.java
 *
 * @version 1.0
 *
 */
public class MyReducer extends Reducer<Text, Text, Text, NullWritable> {
	
	private HashMap<String, String> answers, questions, aqMap;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		answers = new HashMap<>();
		questions = new HashMap<>();
		aqMap = new HashMap<>();
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		String tmp;
		for (String answer : aqMap.keySet()) {
			tmp = aqMap.get(answer) + "," + questions.get(aqMap.get(answer)) + "," + answer + "," + answers.get(answer);
			context.write(new Text(tmp), NullWritable.get());
		}
	}

	@Override
	protected void reduce(
			Text key,
			Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {		
		
		String[] tmp1, tmp2;
		
		for (Text value : values) {
			tmp1 = value.toString().split(":");
			if (tmp1[0].equalsIgnoreCase("answer")) {
				tmp2 = tmp1[1].split(",");
				answers.put(tmp2[0], tmp2[1]);
				aqMap.put(tmp2[0], key.toString());
			} else {
				questions.put(key.toString(), tmp1[1]);
			}
		}
	}
}
