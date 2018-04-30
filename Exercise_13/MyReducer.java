package it.polito.bigdata.hadoop.ex13;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * MyReducer.java
 *
 * @version 1.0
 *
 * Apr 30, 2018
 */
public class MyReducer extends Reducer<NullWritable, MyCustom, Text, Text> {
	
	@Override
	protected void reduce(
			NullWritable key,
			Iterable<MyCustom> values,
			Context context)
			throws IOException, InterruptedException {
		Integer income = Integer.MIN_VALUE;
		String date = "";
		for (MyCustom myCustom : values) {
			if (myCustom.getIncome() > income) {
				income = myCustom.getIncome();
				date = myCustom.getDate();
			}
		}
		context.write(new Text(date), new Text(income.toString()));
	}
}
