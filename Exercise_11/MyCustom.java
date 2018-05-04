package it.polito.bigdata.hadoop.ex11;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MyCustom implements Writable {

	private Double sum;
	private Integer count;
	
	public Double getSum() {
		return sum;
	}

	public void setSum(Double sum) {
		this.sum = sum;
	}

	public Integer getCount() {
		return count;
	}

	public void setCount(Integer count) {
		this.count = count;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		sum = in.readDouble();
		count = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(sum);
		out.writeInt(count);
	}

}
