package lab3b;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import utils.CountStripe;

public class App {

  public static class AppMapper extends Mapper<LongWritable, Text, Text, CountStripe> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] lines = value.toString().split("\n");

      for (String line : lines) {
        String[] items = line.toString().split(" ");

        for (String item : items) {
          CountStripe stripe = new CountStripe();

          for (String nextItem : items) {
            if (!item.equals(nextItem)) {
              Text _key = new Text(nextItem);
              stripe.getStripe().put(_key, new IntWritable(1));
            }
          }
          stripe.setCount(1);
          context.write(new Text(item), stripe);
        }
      }
    }
  }

  public static class AppReducer extends Reducer<Text, CountStripe, Text, Text> {
    public void reduce(Text key, Iterable<CountStripe> values, Context context)
        throws IOException, InterruptedException {
      HashMap<Text, IntWritable> instance = new HashMap<>();
      int thisCount = 0;

      for (CountStripe _stripe : values) {
        for (Text k : _stripe.getStripe().keySet()) {
          if (instance.containsKey(k)) {
            int lastCount = instance.get(k).get();
            int currentCount = _stripe.getStripe().get(k).get();
            int count = lastCount + currentCount;

            instance.put(k, new IntWritable(count));
          } else {
            int currentCount = _stripe.getStripe().get(k).get();
            instance.put(k, new IntWritable(currentCount));
          }
        }
        thisCount += _stripe.getCount();
      }

      HashMap<Text, String> probabilityCoPurchase = new HashMap<>();

      for (Text k : instance.keySet()) {
        int count = instance.get(k).get();
        float prob = (float) count / thisCount;
        DecimalFormat fProb = new DecimalFormat("0.00000");
        // probabilityCoPurchase.put(k, (float) (Math.round(prob * 10000) / 10000));
        probabilityCoPurchase.put(k, fProb.format(prob));
      }

      context.write(key, new Text(probabilityCoPurchase.toString()));
    }

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf);
    job.setJarByClass(App.class);
    job.setMapperClass(AppMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(CountStripe.class);
    job.setReducerClass(AppReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(TextOutputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
