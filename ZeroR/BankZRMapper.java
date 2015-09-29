package BankZR;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Mapper class for ZeroR algorithm of a bank dataset
 * This class extends Mapper class with input key as LongWritable and input value, output key and value as Text
 * <p>
 * @author SOBEHY, Abdallah <a-sobehy@hotmail.com>
 * @author KAUL, Neha <neha.kaul@telecom-sudparis.eu>
 * Template is from <a href="https://training.mapr.com/">MapR Academy: Developing Hadoop Applications course</a>
 */

public class BankZRMapper  extends Mapper <LongWritable,Text,Text,Text> {
	/** String to store the value of the target class */
   String tempString=null;
	
   private static Log log = LogFactory.getLog(BankZRMapper.class);
/**
*	map function takes the input key and value (file records) and writes output key and value for the reduce class.
*	@param key input key
*	@param value file record (one line).
*	@param context used to write the output key and value to the reduce class
*/
   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      /** iterator over input record assuming ','-separated fields*/
      StringTokenizer iterator = new StringTokenizer(value.toString(),",");

      /** store the number of tokens to be used to loop until reaching the target class value.*/ 
	int tokensCount = iterator.countTokens() ;
	
      
      /** Loop iterator until reaching the target class*/
	int i;
	for ( i = 0 ; i < tokensCount ; i ++ )
	{
		tempString = iterator.nextToken().toString();
	}
     
      /** TODO write to the partitioner "target" and the value of the class (yes or no) */
	context.write(new Text ("target"), new Text(tempString) ); 
      
   }

}
