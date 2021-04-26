package lab3a;

import java.io.IOException;
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

import utils.Stripe;

public class App {

  public static class AppMapper extends Mapper<LongWritable, Text, Text, Stripe> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

      String[] lines = value.toString().split("\n");

      for (String line : lines) {
        String[] items = line.toString().split(" ");

        for (String item : items) {
          Stripe stripe = new Stripe();

          for (String _item : items) {
            if (!item.equals(_item)) {
              Text _key = new Text(_item);
              if (stripe.getStripe().containsKey(_key)) {
                int count = ((IntWritable) stripe.getStripe().get(_key)).get();
                stripe.getStripe().put(_key, new IntWritable(count + 1));
              } else {
                stripe.getStripe().put(_key, new IntWritable(1));
              }
            }
          }

          context.write(new Text(item), stripe);
        }
      }

    }
  }

  public static class AppReducer extends Reducer<Text, Stripe, Text, Text> {

    public void reduce(Text key, Iterable<Stripe> values, Context context) throws IOException, InterruptedException {

      Stripe stripe = new Stripe();
      HashMap<Text, IntWritable> instance = stripe.getStripe();

      for (Stripe _stripe : values) {
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
        stripe.setStripe(instance);
      }
      context.write(key, new Text(stripe.getStripe().toString()));
    }

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf);
    job.setJarByClass(App.class);
    job.setMapperClass(AppMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Stripe.class);
    job.setReducerClass(AppReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
