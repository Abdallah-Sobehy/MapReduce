package BankOR; 

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * Driver class for OneRTest to test the accuracy of a certain rule
 * This class extends Configured class and implements Tool
 * <p>
 * @author SOBEHY, Abdallah <a-sobehy@hotmail.com>
 * @author KAUL, Neha <neha.kaul@telecom-sudparis.eu>
 * Template is from <a href="https://training.mapr.com/">MapR Academy: Developing Hadoop Applications course</a>
 */
public class BankORDriver extends Configured implements Tool {
/**
*  run function takes input arguments when running the program
*  Checks there are exactly two arguments, one for the input file of the dataset and the ther for the output directory
*  Throws an exception if the expected values are not provided
*  @param args input arguments
*
*/
   public int run(String[] args) throws Exception {
      // check the CLI
      if (args.length != 2) {
         System.err.println("usage: hadoop jar -classpath $CLASSPATH:BankOR.jar BankOR.BankORDriver <inputfile> <outputdir>");
         System.exit(1);
      }
      /** Setting up the job */ 
      Job job = new Job(getConf());
   /** Set the driver class */
      job.setJarByClass(BankORDriver.class);
      /** Set the mapper class */
      job.setMapperClass(BankORMapper.class);
      /** Set the reducer class */
      job.setReducerClass(BankORReducer.class);
/** Set the input format class to TextInputFormat this means the input to the mapper Key: byte offset of the input file, value: one line text */
      job.setInputFormatClass(TextInputFormat.class);
      /** Set output key class*/
      job.setOutputKeyClass(Text.class);
      /** Set output value class*/
      job.setOutputValueClass(FloatWritable.class);
      /** Set map output value class*/
	job.setMapOutputValueClass(Text.class);
      /** setup input and output paths*/
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1])); 

      /** launch job syncronously*/
      return job.waitForCompletion(true) ? 0 : 1;
   }

   /** main class to launch the job*/
   public static void main(String[] args) throws Exception {
      /** Initializing configuration.*/
      Configuration conf = new Configuration();
      conf.set("mapreduce.output.key.field.separator", ",");
      System.exit(ToolRunner.run(conf, new BankORDriver(), args));
   } 
}
