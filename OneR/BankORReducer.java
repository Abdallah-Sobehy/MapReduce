package BankOR;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
/**
 * Reducer class for OneR algorithm of a bank dataset
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
	/** the percentage of contributon of the age class in describing the target class*/
	float ageContribPercent;
	/** the percentage of contributon of the job class in describing the target class*/
	float jobContribPercent;
	/** the percentage of contributon of the marital class in describing the target class*/
	float maritalContribPercent;
	/** the percentage of contributon of the education class in describing the target class*/
	float educationContribPercent;
	switch (attributeName)// switching classes or columns
	{
		case "age":// first column is age
			/** Stores Yes frequency of target class when the age is young.*/
			float youngYesCount = 0;
			/** Stores No frequency of target class when the age is young.*/
			float youngNoCount = 0;
			/** Stores frequency of the dominant target class value (yes or no) when the age is young.*/
			float youngDomCount = 0;
			/** Stores the value of the dominant value of the target class when the age is young (yes or no).*/
			String youngDomTargetValue = null;
			/** Stores Yes frequency of target class when the age is average.*/
			float averageYesCount = 0;
			/** Stores No frequency of target class when the age is average.*/
			float averageNoCount = 0;
			/** Stores frequency of the dominant target class value (yes or no) when the age is average.*/
			float averageDomCount = 0;
			/** Stores the value of the dominant value of the target class when the age is average (yes or no).*/
			String averageDomTargetValue = null;

			/** Stores Yes frequency of target class when the age is old.*/
			float oldYesCount = 0;
			/** Stores No frequency of target class when the age is old.*/
			float oldNoCount = 0;
			/** Stores frequency of the dominant target class value (yes or no) when the age is old.*/
			float oldDomCount = 0;
			/** Stores the value of the dominant value of the target class when the age is old (yes or no).*/
			String oldDomTargetValue = null;
			/** Looping through all values */ 
			for ( Text value : values)
			{
				recordCount ++ ;
				valueStringArray = value.toString().split("\\s+"); // splitting value to class value and target class value
				switch (valueStringArray[0])
				{
					/** case the value of age is young, update the yes/Nocounts and dominant count and value*/
					case "young":
						if ( valueStringArray [1].equals( "yes"))
							youngYesCount ++;
						if ( valueStringArray [1].equals( "no"))
							youngNoCount ++;
						youngDomTargetValue = youngYesCount >= youngNoCount ? "yes" : "no";
						youngDomCount = youngYesCount >= youngNoCount ? youngYesCount : youngNoCount;
						break;
					/** case the value of age is average, update the yes/Nocounts and dominant count and value*/
					case "average":
						if ( valueStringArray [1].equals( "yes"))
							averageYesCount ++;
						if ( valueStringArray [1].equals( "no"))
							averageNoCount ++;
						averageDomTargetValue = averageYesCount >= averageNoCount ? "yes" : "no";
						averageDomCount = averageYesCount >= averageNoCount ? averageYesCount : averageNoCount;
						break;
						/** case the value of age is old, update the yes/Nocounts and dominant count and value*/
					case "old":
						if ( valueStringArray [1].equals( "yes"))
							oldYesCount ++;
						if ( valueStringArray [1].equals( "no"))
							oldNoCount ++;
						oldDomTargetValue = oldYesCount >= oldNoCount ? "yes" : "no";
						oldDomCount = oldYesCount >= oldNoCount ? oldYesCount : oldNoCount;
						break;

				}
			}
			/** Compute the correct predictions if the dominant target value is expected for all corresponding age values.*/
			ageContribPercent = ( youngDomCount + averageDomCount + oldDomCount ) / recordCount ; 
			/** Write the corresponding rule of prediction.*/
			output = new Text ("AGE: young => " + youngDomTargetValue + " average => " + averageDomTargetValue +" old => " + oldDomTargetValue + "\n");
			context.write(output, new FloatWritable (ageContribPercent) );

			break;

		case "job":
		//type of job (categorical: "admin.","unknownJob","unemployed","management","housemaid","entrepreneur","student",
                                   //    "blue-collar","self-employed","retired","technician","services") 
			float adminYesCount = 0;
			float adminNoCount = 0;
			float adminDomCount = 0;// the count of the dominant value (yes or no)
			String adminDomTargetValue = null;

			float unknownJobYesCount = 0;
			float unknownJobNoCount = 0;
			float unknownJobDomCount = 0;// the count of the dominant value (yes or no)
			String unknownJobDomTargetValue = null;

			float unemployedYesCount = 0;
			float unemployedNoCount = 0;
			float unemployedDomCount = 0;// the count of the dominant value (yes or no)
			String unemployedDomTargetValue = null;

			float managementYesCount = 0;
			float managementNoCount = 0;
			float managementDomCount = 0;// the count of the dominant value (yes or no)
			String managementDomTargetValue = null;

			float housemaidYesCount = 0;
			float housemaidNoCount = 0;
			float housemaidDomCount = 0;// the count of the dominant value (yes or no)
			String housemaidDomTargetValue = null;

			float entrepreneurYesCount = 0;
			float entrepreneurNoCount = 0;
			float entrepreneurDomCount = 0;// the count of the dominant value (yes or no)
			String entrepreneurDomTargetValue = null;

			float studentYesCount = 0;
			float studentNoCount = 0;
			float studentDomCount = 0;// the count of the dominant value (yes or no)
			String studentDomTargetValue = null;
			// blue-collar
			float blueYesCount = 0;
			float blueNoCount = 0;
			float blueDomCount = 0;// the count of the dominant value (yes or no)
			String blueDomTargetValue = null;
			// self employed
			float selfYesCount = 0;
			float selfNoCount = 0;
			float selfDomCount = 0;// the count of the dominant value (yes or no)
			String selfDomTargetValue = null;

			float retiredYesCount = 0;
			float retiredNoCount = 0;
			float retiredDomCount = 0;// the count of the dominant value (yes or no)
			String retiredDomTargetValue = null;

			float technicianYesCount = 0;
			float technicianNoCount = 0;
			float technicianDomCount = 0;// the count of the dominant value (yes or no)
			String technicianDomTargetValue = null;

			float servicesYesCount = 0;
			float servicesNoCount = 0;
			float servicesDomCount = 0;// the count of the dominant value (yes or no)
			String servicesDomTargetValue = null;

			for ( Text value : values)
			{
				recordCount ++ ;
				valueStringArray = value.toString().split("\\s+"); // splitting value to class value and target class value
				switch (valueStringArray[0])
				{
					case "admin.":
						if ( valueStringArray [1].equals( "yes"))
							adminYesCount ++;
						if ( valueStringArray [1].equals( "no"))
							adminNoCount ++;
						adminDomTargetValue = adminYesCount >= adminNoCount ? "yes" : "no";
						adminDomCount = adminYesCount >= adminNoCount ? adminYesCount : adminNoCount;
						break;

					case "unknown":
						if ( valueStringArray [1].equals( "yes"))
							unknownJobYesCount ++;
						if ( valueStringArray [1].equals( "no"))
							unknownJobNoCount ++;
						unknownJobDomTargetValue = unknownJobYesCount >= unknownJobNoCount ? "yes" : "no";
						unknownJobDomCount = unknownJobYesCount >= unknownJobNoCount ? unknownJobYesCount : unknownJobNoCount;
						break;

					case "unemployed":
						if ( valueStringArray [1].equals( "yes"))
							unemployedYesCount ++;
						if ( valueStringArray [1].equals( "no"))
							unemployedNoCount ++;
						unemployedDomTargetValue = unemployedYesCount >= unemployedNoCount ? "yes" : "no";
						unemployedDomCount = unemployedYesCount >= unemployedNoCount ? unemployedYesCount : unemployedNoCount;
						break;

					case "management":
						if ( valueStringArray [1].equals( "yes"))
							managementYesCount ++;
						if ( valueStringArray [1].equals( "no"))
							managementNoCount ++;
						managementDomTargetValue = managementYesCount >= managementNoCount ? "yes" : "no";
						managementDomCount = managementYesCount >= managementNoCount ? managementYesCount : managementNoCount;
						break;

					case "housemaid":
						if ( valueStringArray [1].equals( "yes"))
							housemaidYesCount ++;
						if ( valueStringArray [1].equals( "no"))
							housemaidNoCount ++;
						housemaidDomTargetValue = housemaidYesCount >= housemaidNoCount ? "yes" : "no";
						housemaidDomCount = housemaidYesCount >= housemaidNoCount ? housemaidYesCount : housemaidNoCount;
						break;

					case "entrepreneur":
						if ( valueStringArray [1].equals( "yes"))
							entrepreneurYesCount ++;
						if ( valueStringArray [1].equals( "no"))
							entrepreneurNoCount ++;
						entrepreneurDomTargetValue = entrepreneurYesCount >= entrepreneurNoCount ? "yes" : "no";
						entrepreneurDomCount = entrepreneurYesCount >= entrepreneurNoCount ? entrepreneurYesCount : entrepreneurNoCount;
						break;

					case "student":
						if ( valueStringArray [1].equals( "yes"))
							studentYesCount ++;
						if ( valueStringArray [1].equals( "no"))
							studentNoCount ++;
						studentDomTargetValue = studentYesCount >= studentNoCount ? "yes" : "no";
						studentDomCount = studentYesCount >= studentNoCount ? studentYesCount : studentNoCount;
						break;

					case "blue-collar":
						if ( valueStringArray [1].equals( "yes"))
							blueYesCount ++;
						if ( valueStringArray [1].equals( "no"))
							blueNoCount ++;
						blueDomTargetValue = blueYesCount >= blueNoCount ? "yes" : "no";
						blueDomCount = blueYesCount >= blueNoCount ? blueYesCount : blueNoCount;
						break;

					case "self-employed":
						if ( valueStringArray [1].equals( "yes"))
							selfYesCount ++;
						if ( valueStringArray [1].equals( "no"))
							selfNoCount ++;
						selfDomTargetValue = selfYesCount >= selfNoCount ? "yes" : "no";
						selfDomCount = selfYesCount >= selfNoCount ? selfYesCount : selfNoCount;
						break;


					case "retired":
						if ( valueStringArray [1].equals( "yes"))
							retiredYesCount ++;
						if ( valueStringArray [1].equals( "no"))
							retiredNoCount ++;
						retiredDomTargetValue = retiredYesCount >= retiredNoCount ? "yes" : "no";
						retiredDomCount = retiredYesCount >= retiredNoCount ? retiredYesCount : retiredNoCount;
						break;

					case "technician":
						if ( valueStringArray [1].equals( "yes"))
							technicianYesCount ++;
						if ( valueStringArray [1].equals( "no"))
							technicianNoCount ++;
						technicianDomTargetValue = technicianYesCount >= technicianNoCount ? "yes" : "no";
						technicianDomCount = technicianYesCount >= technicianNoCount ? technicianYesCount : technicianNoCount;
						break;

					case "services":
						if ( valueStringArray [1].equals( "yes"))
							servicesYesCount ++;
						if ( valueStringArray [1].equals( "no"))
							servicesNoCount ++;
						servicesDomTargetValue = servicesYesCount >= servicesNoCount ? "yes" : "no";
						servicesDomCount = servicesYesCount >= servicesNoCount ? servicesYesCount : servicesNoCount;
						break;		
				}
			}

			jobContribPercent = ( adminDomCount + unknownJobDomCount + unemployedDomCount + managementDomCount + housemaidDomCount + entrepreneurDomCount + studentDomCount + blueDomCount + selfDomCount + retiredDomCount + technicianDomCount + servicesDomCount ) / recordCount ; 
			output = new Text ("JOB: admin => " + adminDomTargetValue + " unknownJob => " + unknownJobDomTargetValue +" unemployed => " + unemployedDomTargetValue + "\n management => " + managementDomTargetValue + " housemaid => " + housemaidDomTargetValue + " entrepreneur => " + entrepreneurDomTargetValue + " \n student => " + studentDomTargetValue + "blue-collar => " + blueDomTargetValue + " self-employed => " + selfDomTargetValue + " retired => " + retiredDomTargetValue + " technician => " + technicianDomTargetValue + " services => " + servicesDomTargetValue + "\n");
			context.write(output, new FloatWritable (jobContribPercent) );				

			break;

			case "marital":
			//marital status (categorical: "married","divorced","single"; note: "divorced" means divorced or widowed)

			float marriedYesCount = 0;
			float marriedNoCount = 0;
			float marriedDomCount = 0;// the count of the dominant value (yes or no)
			String marriedDomTargetValue = null;

			float divorcedYesCount = 0;
			float divorcedNoCount = 0;
			float divorcedDomCount = 0;// the count of the dominant value (yes or no)
			String divorcedDomTargetValue = null;

			float singleYesCount = 0;
			float singleNoCount = 0;
			float singleDomCount = 0;// the count of the dominant value (yes or no)
			String singleDomTargetValue = null;

			for ( Text value : values)
			{
				recordCount ++ ;
				valueStringArray = value.toString().split("\\s+"); // splitting value to class value and target class value
				switch (valueStringArray[0])
				{
					case "married":
						if ( valueStringArray [1].equals( "yes"))
							marriedYesCount ++;
						if ( valueStringArray [1].equals( "no"))
							marriedNoCount ++;
						marriedDomTargetValue = marriedYesCount >= marriedNoCount ? "yes" : "no";
						marriedDomCount = marriedYesCount >= marriedNoCount ? marriedYesCount : marriedNoCount;
						break;

					case "divorced":
						if ( valueStringArray [1].equals( "yes"))
							divorcedYesCount ++;
						if ( valueStringArray [1].equals( "no"))
							divorcedNoCount ++;
						divorcedDomTargetValue = divorcedYesCount >= divorcedNoCount ? "yes" : "no";
						divorcedDomCount = divorcedYesCount >= divorcedNoCount ? divorcedYesCount : divorcedNoCount;
						break;

					case "single":
						if ( valueStringArray [1].equals( "yes"))
							singleYesCount ++;
						if ( valueStringArray [1].equals( "no"))
							singleNoCount ++;
						singleDomTargetValue = singleYesCount >= singleNoCount ? "yes" : "no";
						singleDomCount = singleYesCount >= singleNoCount ? singleYesCount : singleNoCount;
						break;
				}
			}
			maritalContribPercent = ( marriedDomCount + divorcedDomCount + singleDomCount ) / recordCount ; 
			output = new Text ("MARITAL: married => " + marriedDomTargetValue + " divorced => " + divorcedDomTargetValue +" single => " + singleDomTargetValue + "\n");
			context.write(output, new FloatWritable (maritalContribPercent) );
			break;

			case "education":
			// (categorical: "unknownEdu","secondary","primary","tertiary")
			float unknownEduYesCount = 0;
			float unknownEduNoCount = 0;
			float unknownEduDomCount = 0;// the count of the dominant value (yes or no)
			String unknownEduDomTargetValue = null;

			float primaryYesCount = 0;
			float primaryNoCount = 0;
			float primaryDomCount = 0;// the count of the dominant value (yes or no)
			String primaryDomTargetValue = null;

			float secondaryYesCount = 0;
			float secondaryNoCount = 0;
			float secondaryDomCount = 0;// the count of the dominant value (yes or no)
			String secondaryDomTargetValue = null;

			float tertiaryYesCount = 0;
			float tertiaryNoCount = 0;
			float tertiaryDomCount = 0;// the count of the dominant value (yes or no)
			String tertiaryDomTargetValue = null;

			for ( Text value : values)
			{
				recordCount ++ ;
				valueStringArray = value.toString().split("\\s+"); // splitting value to class value and target class value
				switch (valueStringArray[0])
				{
					case "unknown":

						if ( valueStringArray [1].equals( "yes"))
							unknownEduYesCount ++;
						if ( valueStringArray [1].equals( "no"))
							unknownEduNoCount ++;
						unknownEduDomTargetValue = unknownEduYesCount >= unknownEduNoCount ? "yes" : "no";
						unknownEduDomCount = unknownEduYesCount >= unknownEduNoCount ? unknownEduYesCount : unknownEduNoCount;
						break;

					case "primary":

						if ( valueStringArray [1].equals( "yes"))
							primaryYesCount ++;
						if ( valueStringArray [1].equals( "no"))
							primaryNoCount ++;
						primaryDomTargetValue = primaryYesCount >= primaryNoCount ? "yes" : "no";
						primaryDomCount = primaryYesCount >= primaryNoCount ? primaryYesCount : primaryNoCount;
						break;	

					case "secondary":

						if ( valueStringArray [1].equals( "yes"))
							secondaryYesCount ++;
						if ( valueStringArray [1].equals( "no"))
							secondaryNoCount ++;
						secondaryDomTargetValue = secondaryYesCount >= secondaryNoCount ? "yes" : "no";
						secondaryDomCount = secondaryYesCount >= secondaryNoCount ? secondaryYesCount : secondaryNoCount;
						break;			

					case "tertiary":

						if ( valueStringArray [1].equals( "yes"))
							tertiaryYesCount ++;
						if ( valueStringArray [1].equals( "no"))
							tertiaryNoCount ++;
						tertiaryDomTargetValue = tertiaryYesCount >= tertiaryNoCount ? "yes" : "no";
						tertiaryDomCount = tertiaryYesCount >= tertiaryNoCount ? tertiaryYesCount : tertiaryNoCount;
						break;
				}
			}
			educationContribPercent = ( unknownEduDomCount + primaryDomCount + secondaryDomCount + tertiaryDomCount ) / recordCount ; 
			output = new Text ("EDUCATION: unknown => " + unknownEduDomTargetValue + " primary => " + primaryDomTargetValue + " secondary => " + secondaryDomTargetValue +" tertiary => " + tertiaryDomTargetValue + "\n");
			context.write(output, new FloatWritable (educationContribPercent) );
			break;
	}

    }     
}
