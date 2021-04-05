package utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Stripe implements Writable {
  private HashMap<Text, IntWritable> stripe;

  public Stripe() {
    this.stripe = new HashMap<Text, IntWritable>();
  }

  public HashMap<Text, IntWritable> getStripe() {
    return stripe;
  }

  public void setStripe(HashMap<Text, IntWritable> stripe) {
    this.stripe = stripe;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(stripe.size());

    for (Text key : stripe.keySet()) {
      key.write(out);
      stripe.get(key).write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    stripe = new HashMap<Text, IntWritable>();
    int size = in.readInt();

    for (int i = 0; i < size; i++) {
      Text key = new Text();
      IntWritable value = new IntWritable();

      key.readFields(in);
      value.readFields(in);

      stripe.put(key, value);
    }
  }

  public String toString() {
    return this.stripe.toString();
  }

}
