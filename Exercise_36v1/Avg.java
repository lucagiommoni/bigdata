package it.polito.bigdata.spark.ex36;

public class Avg {
	private double sum;
	private double count;
	
	public Avg(double sum, double count) {	
		this.sum = sum;
		this.count = count;
	}
	public double getSum() {
		return sum;
	}
	public void setSum(double sum) {
		this.sum = sum;
	}
	public double getCount() {
		return count;
	}
	public void setCount(double count) {
		this.count = count;
	}
	public double computeAvg() {
		return sum/count;
	}
}
