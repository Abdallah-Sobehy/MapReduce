package BankOR;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.IOException;
import java.util.StringTokenizer;
/**
 * Mapper class to test OneR algorithm accuracy of a bank dataset
 * This class extends Mapper class with input key as LongWritable and input value, output key and value as Text
 * <p>
 * @author SOBEHY, Abdallah <a-sobehy@hotmail.com>
 * @author KAUL, Neha <neha.kaul@telecom-sudparis.eu>
 * Template is from <a href="https://training.mapr.com/">MapR Academy: Developing Hadoop Applications course</a>
 */
public class BankORMapper  extends Mapper <LongWritable,Text,Text,Text> {
	/** String to store temporary strings */
   String tempString=null;
  /** Store taarget class value.*/
	String targetValue = null;
   private static Log log = LogFactory.getLog(BankORMapper.class);
/**
*	map function takes the input key and value (file records) and writes output key and value for the reduce class.
*	@param key input key
*	@param value file record (one line).
*	@param context used to write the output key and value to the reduce class
*/
   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      /** iterator over input record assuming ','-separated fields*/
      StringTokenizer iterator = new StringTokenizer(value.toString(),",");

    /** Stores the number of fields.*/ 
	int tokensCount = iterator.countTokens() ;
	
      
    /** Loop iterator until reaching the target class and save its value in targetValue String.*/
	int i;
	for ( i = 0 ; i < tokensCount ; i ++ )
	{
		targetValue = iterator.nextToken().toString();
	}

	/** Iterate again for each class class except the target class*/
	iterator = new StringTokenizer(value.toString(),",");

	for ( i = 0 ; i < tokensCount - 1 ; i ++ )
	{
		tempString = iterator.nextToken().toString();
		switch (i)
		{
			/** First column: age, age is put into one of three categories: young, old, average.*/
			case 0 :
				String ageCategory = null;
				Integer ageInteger = new Integer(tempString);
      			int ageInt = ageInteger.intValue();
				if ( ageInt < 30 )
					ageCategory = "young";
				else if (ageInt > 50 ) 
					ageCategory = "old";
				else ageCategory = "average";
				/** Write the key to be age and the corresponding value */
				context.write( new Text ("age"), new Text (ageCategory+" " + targetValue));
				break;

			// Second column: job
			case 1 :
				context.write( new Text ("job"), new Text (tempString+" " + targetValue));
				break;
			// Third column: marital status (categorical: "married","divorced","single"; note: "divorced" means divorced or widowed)
			case 2 :
				context.write( new Text ("marital"), new Text (tempString+" " + targetValue));
				break;


			// forth column: education
			case 3 :
				context.write( new Text ("education"), new Text (tempString+" " + targetValue));
				break;

			// fifth column: has credit yes or no
			case 4 :
				context.write( new Text ("credit"), new Text (tempString+" " + targetValue));
				break;

			// seventh column: housing yes or no
			case 6 :
				context.write( new Text ("housing"), new Text (tempString+" " + targetValue));
				break;

			// Eigth column: loan status
			case 7 :
				context.write( new Text ("loan"), new Text (tempString+" " + targetValue));
				break;

			// Ninth column: contact
			case 8 :
				context.write( new Text ("contact"), new Text (tempString+" " + targetValue));
				break;

			// Eleventh column: last contact month
			case 10 :
				context.write( new Text ("month"), new Text (tempString+" " + targetValue));
				break;

			// sixteenth column: poutcome outcome of the previous marketing campaign
			case 15 :
				context.write( new Text ("poutcome"), new Text (tempString+" " + targetValue));
				break;

		} 
	}
	 
         
   }

}
