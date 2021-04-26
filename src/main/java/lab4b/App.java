package lab4b;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import utils.Couple;

public class App {

  public static class AppMapper extends Mapper<LongWritable, Text, Text, Couple> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] tokens = value.toString().split(",");

      if (tokens[1].equals("gia"))
        return;

      String skey = tokens.length > 4 ? tokens[0] + "," + tokens[1] : tokens[0];
      String sPrice = tokens.length > 4 ? tokens[tokens.length - 3] : tokens[1];
      int price = (int) Float.parseFloat(sPrice);
      Couple couple = new Couple(price);

      context.write(new Text(skey), couple);
    }
  }

  public static class AppReducer extends Reducer<Text, Couple, Text, FloatWritable> {
    public void reduce(Text key, Iterable<Couple> values, Context context) throws IOException, InterruptedException {
      int count = 0;
      int sum = 0;

      for (Couple value : values) {
        int p = value.getPrice().get();
        int c = value.getCount().get();

        count += c;
        sum += p;
      }

      context.write(key, new FloatWritable(sum / count));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf);
    job.setJarByClass(App.class);
    job.setMapperClass(AppMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Couple.class);
    job.setReducerClass(AppReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(TextOutputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
