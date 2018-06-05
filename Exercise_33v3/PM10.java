package it.polito.bigdata.spark.ex33;

import java.io.Serializable;

@SuppressWarnings("serial")
public class PM10 implements Serializable {

	private Double pm10val;
	
	public PM10(double pm10val) {
		this.pm10val = pm10val;
	}
	public Double getPm10val() {
		return pm10val;
	}
	public void setPm10val(Double pm10val) {
		this.pm10val = pm10val;
	}
	@Override
	public String toString() {
		return pm10val.toString();
	}
}
