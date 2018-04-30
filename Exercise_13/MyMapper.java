package it.polito.bigdata.hadoop.ex13;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * MyMapper.java
 *
 * @version 1.0
 *
 * Apr 30, 2018
 */
public class MyMapper extends Mapper<Text, Text, NullWritable, MyCustom> {

	private MyCustom myCustom;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		this.myCustom = new MyCustom();
		myCustom.setIncome(Integer.MIN_VALUE);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(NullWritable.get(), myCustom);
	}

	@Override
	protected void map(
			Text key,
			Text value,
			Context context)
			throws IOException, InterruptedException {
		int tmpIncome = Integer.parseInt(value.toString());
		if (tmpIncome > this.myCustom.getIncome()) {
			this.myCustom.setIncome(tmpIncome);
			this.myCustom.setDate(key.toString());
		}
	}

}
