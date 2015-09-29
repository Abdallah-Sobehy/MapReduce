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

/**
 * Test class for OneRTest mapper and reducer
 * <p>
 * @author SOBEHY, Abdallah <a-sobehy@hotmail.com>
 * @author KAUL, Neha <neha.kaul@telecom-sudparis.eu>
 * Template is from <a href="https://training.mapr.com/">MapR Academy: Developing Hadoop Applications course</a>
 */
public class BankORTest {
	   /** Declare mapperDriver object.*/
   private static MapDriver<LongWritable, Text, Text, Text> mapDriver;
      /** Declare reducerDriver object.*/
   private static ReduceDriver<Text, Text, Text, FloatWritable> reduceDriver;

   /** Set up before testing */
   private static void setUp() {
   	/** instantiate mapper object and mapper driver*/
      BankOR.BankORMapper mapper = new BankOR.BankORMapper();
      mapDriver = MapDriver.newMapDriver(mapper);
      /** instantiate a reducer object and reducer driver*/
      BankOR.BankORReducer reducer = new BankOR.BankORReducer();
      reduceDriver = ReduceDriver.newReduceDriver(reducer);
   }

   /* Testing the mapper by comparing its output to an expected output written in the file mymaptest.dat
   * @param key input key to the mapper
   * @param value input value to the mapper
   * @param output expected output
   */
   private static void testMapper(LongWritable key, Text value, String output) throws IOException {
   	/** Splitting the expected output into key and value pair to be able to match them against the mapper's output*/
      String[] outputStringArray = output.split(" ");
    /** Expected output key.*/
      String outputKey = outputStringArray[0];
    /** Expected output value.*/
      String outputValue = outputStringArray[1]+ " "+ outputStringArray[2];
    /** Calling mapper and comparing its output with the expected one.*/
      mapDriver
         .withInput(key, value)
         .withOutput(new Text(outputKey), new Text(outputValue)) 
         .runTest();
      }

  /* Testing the reducer by comparing its output to an expected output in the file myreducetest.dat
   * @param key input key to the reducer
   * @param values input list of values to the reducer
   * @param output expected output
   */
   private static void testReducer(Text key, List<Text> values, String output) throws IOException {
   /** Splitting the expected output into key and value pair to be able to match them against the reducer's output*/
      String[] outputStringArray = output.split("\\s+");
      /** Expected output key.*/
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
      /** 
      * main function tests mapper or reducer depending on the input arguments.
      */
   public static void main(String[] args) {
   /* check two arguments are provided: first to indicate which function to test, second: the file storing expected output */
      if (args.length != 2) {
        System.err.printf("usage: %s <map | reduce> <inputfile>\n", "ReceiptsTest"); 
        System.exit(1);
      }
      
      String value=null, output=null;
      BufferedReader reader;
      try {
      	 /** read the test file of expected values.*/
         reader = new BufferedReader(new FileReader(args[1]));
		/** First line is the input to the mapper/reducer.*/
         value = reader.readLine();
        /** Second line contains the expected output.*/
         output = reader.readLine();
      } 
      catch(IOException e) {System.out.println("error reading from input file " + e.toString());}

      /** Testing mapper.*/
      setUp();
      if (args[0].equals("map")) {
         /** Call Test Mapper function with the input value and expected output.*/
         try { testMapper(new LongWritable(0), new Text(value), output);}
         catch (Exception e) {System.err.println("error running test: " + value.toString() + " " + output);}
         finally {System.out.println("success");} 
      }
     	/** Testing reducer.*/
         else if (args[0].equals("reduce")) {
         /** list object for reducer input*/
         List<Text> reduceInput = new ArrayList<Text>();
        /** tokenize the first line from the input file.*/ 
         StringTokenizer iterator = new StringTokenizer(value, " ");
         /** pull out the key from the tokenized line, which is the first value.*/
         Text key = new Text(iterator.nextToken());
         /**loop through tokens to add Text to reduce input list*/
         while(iterator.hasMoreTokens()) 
            reduceInput.add(new Text (iterator.nextToken()) );
         /** Call Test Mapper function with the input value and expected output.*/
         try { testReducer(key, reduceInput, output);}
         catch (Exception e) {System.err.println("error running test: " + value.toString() + " " + output);}
         finally {System.out.println("success");} 
      }

      return;
   }
} 
