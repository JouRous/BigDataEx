package lab4c;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class App {
  public static class AppMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] tokens = value.toString().split(",");

      if (tokens[1].equals("gia"))
        return;

      String skey = tokens.length > 4 ? tokens[0] + "," + tokens[1] : tokens[0];
      String sPrice = tokens.length > 4 ? tokens[tokens.length - 3] : tokens[1];
      int price = (int) Float.parseFloat(sPrice);

      context.write(new Text(skey), new IntWritable(price));
    }
  }

  public static class AppReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

      int max = Integer.MIN_VALUE;
      int min = Integer.MAX_VALUE;

      for (IntWritable value : values) {
        if (value.get() > max) {
          max = value.get();
        }
        if (value.get() < min) {
          min = value.get();
        }
      }
      context.write(new Text(key.toString() + " max: "), new IntWritable(max));
      context.write(new Text(key.toString() + " min: "), new IntWritable(min));

    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf);
    job.setJarByClass(App.class);
    job.setMapperClass(AppMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setReducerClass(AppReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(TextOutputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
