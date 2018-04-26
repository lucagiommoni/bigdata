/**
 * 
 */
package it.polito.bigdata.hadoop.ex5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * MyCustomType.java
 *
 * @version 1.0
 *
 * Apr 24, 2018
 */
public class MyCustomType implements Writable {
	
	private float sum = 0;
	private int count = 0;
	
	public float getSum() {
		return sum;
	}

	public void setSum(float sum) {
		this.sum = sum;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.sum = in.readFloat();
		this.count = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(sum);
		out.writeInt(count);
	}

}
