package BankOR;

import org.junit.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

public class BankORTest {
   private static MapDriver<LongWritable, Text, Text, Text> mapDriver;
   // TODO declare the reduceDriver object
   private static ReduceDriver<Text, Text, Text, FloatWritable> reduceDriver;

   @Before
   private static void setUp() {
      BankOR.BankORMapper mapper = new BankOR.BankORMapper();
      mapDriver = MapDriver.newMapDriver(mapper);
      // TODO instantiate a reducer object and reducer driver
      BankOR.BankORReducer reducer = new BankOR.BankORReducer();
      reduceDriver = ReduceDriver.newReduceDriver(reducer);
   }

   @Test
   private static void testMapper(LongWritable key, Text value, String output) throws IOException {
      String[] outputStringArray = output.split(" ");
      String outputKey = outputStringArray[0];
      String outputValue = outputStringArray[1]+ " "+ outputStringArray[2];
      mapDriver
         .withInput(key, value)
         .withOutput(new Text(outputKey), new Text(outputValue)) 
         .runTest();
      }

   @Test
   private static void testReducer(Text key, List<Text> values, String output) throws IOException {
   // TODO implement the testReducer method
      String[] outputStringArray = output.split("\\s+");
      int i;
      for ( i = 0 ; i < outputStringArray.length - 1 ; i++)
      {
      	String outputKey += outputStringArray[i] + " ";	
      }
      
      String outputValue = outputStringArray[outputStringArray.length - 1];
      reduceDriver
         .withInput(key, values)
         .withOutput(new Text(outputKey), new FloatWritable(Float.parseFloat(outputValue))) 
         .runTest();
      }

   public static void main(String[] args) {
      if (args.length != 2) {
        System.err.printf("usage: %s <map | reduce> <inputfile>\n", "ReceiptsTest"); 
        System.exit(1);
      }
      
      String value=null, output=null;
      BufferedReader reader;
      try {
         reader = new BufferedReader(new FileReader(args[1]));
         value = reader.readLine();
         output = reader.readLine();
      } 
      catch(IOException e) {System.out.println("error reading from input file " + e.toString());}

    
      setUp();
      if (args[0].equals("map")) {
         try { testMapper(new LongWritable(0), new Text(value), output);}
         catch (Exception e) {System.err.println("error running test: " + value.toString() + " " + output);}
         finally {System.out.println("success");} 
      }
     
         else if (args[0].equals("reduce")) {
         // TODO create a list object for reduce input
         List<Text> reduceInput = new ArrayList<Text>();
         // TODO tokenize the first line from the input file 
         StringTokenizer iterator = new StringTokenizer(value, " ");
         // TODO pull out the key from the tokenized line
         Text key = new Text(iterator.nextToken());
         // TODO loop through tokens to add Text to reduce input list
         while(iterator.hasMoreTokens()) 
            reduceInput.add(new Text (iterator.nextToken()) );

         try { testReducer(key, reduceInput, output);}
         catch (Exception e) {System.err.println("error running test: " + value.toString() + " " + output);}
         finally {System.out.println("success");} 
      }

      return;
   }
} 
