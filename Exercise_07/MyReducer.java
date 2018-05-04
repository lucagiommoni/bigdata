package it.polito.bigdata.hadoop.ex7;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * MyReducer.java
 *
 * @version 1.0
 *
 * Apr 24, 2018
 */
public class MyReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {}

	@Override
	protected void reduce(
			Text key,
			Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
		
		LinkedList<String> idsList = new LinkedList<>();
		String ids = "";
		
		for (Text value : values) {
			String[] tmp = value.toString().split(",");
			for (String id : tmp) {
				if (!idsList.contains(id)) {
					idsList.add(id);
				}
			}
		}
		
		for (String id : idsList) {
			ids += id + ",";
		}
		
		ids = ids.substring(0,ids.length()-1);
		context.write(key, new Text(ids));
	}
}
