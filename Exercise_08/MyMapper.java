package it.polito.bigdata.hadoop.ex8;

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
public class MyMapper extends Mapper<Text, Text, Text, MyCustom> {

	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, MyCustom>.Context context)
			throws IOException, InterruptedException {
		MyCustom myCustom = new MyCustom();
		String[] tmp = key.toString().split("-");
		myCustom.setMonth(tmp[1]);
		myCustom.setIncome(Integer.parseInt(value.toString()));
		context.write(new Text(tmp[0]), myCustom);
	}
}
