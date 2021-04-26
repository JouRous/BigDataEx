package utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class Couple implements Writable {

  private IntWritable price;
  private IntWritable count;

  public Couple() {
    this.price = new IntWritable(0);
    this.count = new IntWritable(0);
  }

  public Couple(int price) {
    this.price = new IntWritable(price);
    this.count = new IntWritable(1);
  }

  public void write(DataOutput out) throws IOException {
    price.write(out);
    count.write(out);
  }

  public void readFields(DataInput in) throws IOException {
    price.readFields(in);
    count.readFields(in);
  }

  public IntWritable getCount() {
    return count;
  }

  public IntWritable getPrice() {
    return price;
  }
}
