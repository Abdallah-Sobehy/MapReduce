package BankZR;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;

/**
 * Reducer class for ZeroR algorithm of a bank dataset
 * This class extends Reducer class with input key, input value, output key as Text and output value as FloatWritable
 * <p>
 * @author SOBEHY, Abdallah <a-sobehy@hotmail.com>
 * @author KAUL, Neha <neha.kaul@telecom-sudparis.eu>
 * Template is from <a href="https://training.mapr.com/">MapR Academy: Developing Hadoop Applications course</a>
 */
public class BankZRReducer  extends Reducer <Text,Text,Text,FloatWritable> {
/**
*	reduce function takes the input key and iterable set of values and writes the output.
*	@param key input key
*	@param values iterable Text values associated with the key.
*	@param context used to write the output key and value to the output file
*/
   public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
   	/** an integer counts the total number of records */
	float recordCount = 0;
	/** an integer to store the frequency of 'yes' in the target class*/
	float yesCount = 0 ;
	/** an integer to store the frequency of 'no' in the target class*/
	float noCount = 0;
	/** an integer to store the percentage of the dominant value of target class (yes or no) */
	float percent;
	/** Looping through all values */
	for ( Text value: values )
	{
		/** increment yes or no counts depending on the value of the target class */
		if ( value.toString().equals("yes"))
			yesCount++;
		if ( value.toString().equals("no") )
			noCount ++;
		recordCount++;
	}
	/** Write the percentage of the dominant value on the output file */
	if ( yesCount >= noCount )
	{
		percent = (yesCount/recordCount) * 100;
		context.write( new Text ("yes") , new FloatWritable (percent) );
	}
	else 
	{
		percent = (noCount/ recordCount)*100;
		context.write( new Text ("no") , new FloatWritable(percent) ); 
	}
    }     
}
