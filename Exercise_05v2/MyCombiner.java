package it.polito.bigdata.hadoop.ex5;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * MyCombiner.java
 *
 * @version 1.0
 *
 * Apr 24, 2018
 */
public class MyCombiner extends Reducer<Text, MyCustomType, Text, MyCustomType> {

	@Override
	protected void reduce(Text key, Iterable<MyCustomType> value,
			Reducer<Text, MyCustomType, Text, MyCustomType>.Context context) throws IOException, InterruptedException {
		MyCustomType cAvg = new MyCustomType();
		for (MyCustomType customAvg : value) {
			cAvg.setCount(cAvg.getCount() + customAvg.getCount());
			cAvg.setSum(cAvg.getSum() + customAvg.getSum());
		}
		context.write(key, cAvg);
	}
}
