package BankOR; 

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class BankORDriver extends Configured implements Tool {

   public int run(String[] args) throws Exception {
      // check the CLI
      if (args.length != 2) {
         System.err.println("usage: hadoop jar -classpath $CLASSPATH:BankOR.jar BankOR.BankORDriver <inputfile> <outputdir>");
         System.exit(1);
      }
      // setup the Job   
      Job job = new Job(getConf());
 
      job.setJarByClass(BankORDriver.class);
      job.setMapperClass(BankORMapper.class);
      job.setReducerClass(BankORReducer.class);
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
	job.setMapOutputValueClass(Text.class);
      // setup input and output paths
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1])); 

      // launch job syncronously
      return job.waitForCompletion(true) ? 0 : 1;


   }

   public static void main(String[] args) throws Exception { 
      Configuration conf = new Configuration();
      conf.set("mapreduce.output.key.field.separator", ",");
      System.exit(ToolRunner.run(conf, new BankORDriver(), args));
   } 
}
