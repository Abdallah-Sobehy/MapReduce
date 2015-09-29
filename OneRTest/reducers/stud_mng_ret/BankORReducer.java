package BankOR;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
/**
 * Reducer class for OneRTest to test accuracy of algorithm of a bank dataset
 * This class extends Reducer class with input key, input value, output key as Text and output value as FloatWritable
 * <p>
 * @author SOBEHY, Abdallah <a-sobehy@hotmail.com>
 * @author KAUL, Neha <neha.kaul@telecom-sudparis.eu>
 * Template is from <a href="https://training.mapr.com/">MapR Academy: Developing Hadoop Applications course</a>
 */

public class BankORReducer  extends Reducer <Text,Text,Text,FloatWritable> {
/**
*	reduce function takes the input key and iterable set of values and writes the output.
*	@param key input key
*	@param values iterable Text values associated with the key.
*	@param context used to write the output key and value to the output file
*/
   public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
   	/** an integer counts the total number of records */
	float recordCount = 0; 
	/** Stores the title of the field: job, education, marital ...*/
	String attributeName = key.toString();
	/** Stores the values of the key that is divided into two strings: field value and target class value.*/
	String[] valueStringArray;
	/** Stores the output to be written to the output file.*/
	Text output = null;
	/** The rule tested is Predict Yes for student, management and retired, No otherwise*/
	if (attributeName.equals("job"))// switching classes or columns
	{
		//type of job (categorical: "admin.","unknownJob","unemployed","management","housemaid","entrepreneur","student",
                                   //    "blue-collar","self-employed","retired","technician","services") 
		/** holds the number of correct predictions.*/
		float trueExpectations = 0;
		/** olds the number of false predictions*/
		float falseExpectations = 0;
		/** The ratio of correct predictions.*/
		float predictionAccuracy;
			for ( Text value : values)
			{
				recordCount ++ ;
				valueStringArray = value.toString().split("\\s+"); // splitting value to class value and target class value
				switch (valueStringArray[0])
				{

					case "student":
					case "management":
					case "retired":
						if ( valueStringArray [1].equals( "yes"))
							trueExpectations ++;
						if ( valueStringArray [1].equals( "no"))
							falseExpectations ++;
					break;

					default:
					if ( valueStringArray [1].equals( "yes"))
							falseExpectations ++;
						if ( valueStringArray [1].equals( "no"))
							trueExpectations ++;
					break;

				}
			}
			predictionAccuracy = trueExpectations/recordCount;
			output = new Text ("Using the rule JOB: student, management, retired => yes\n Other jobs => NO\n");
			context.write(output, new FloatWritable (predictionAccuracy) );				
	}

    }     
}
