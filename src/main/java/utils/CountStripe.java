package utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CountStripe extends Stripe {
  private int count;

  public CountStripe() {
    super();
    count = 1;
  }

  public void setCount(int count) {
    this.count = count;
  }

  public int getCount() {
    return count;
  }

  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.count = in.readInt();
  }

  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(this.count);
  }

  public String toString() {
    String str = this.count + this.getStripe().toString();
    return str;
  }
}
