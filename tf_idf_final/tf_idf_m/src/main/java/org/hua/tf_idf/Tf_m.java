
package org.hua.tf_idf;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class Tf_m {
       public static class TCDocMapper extends Mapper<Object, Text, Text, IntWritable> {
 
       @Override
        protected void map(Object  key, Text value, Context context) throws IOException, InterruptedException 
        {
 /**
     * @param key
     * @param value-Text contains all the words in a documents
     * @param context contains the context of the job run
     *
     *             PRECONDITION: receive a list of <Object, Text>--
     *             POSTCONDITION: <"word,doc>,  1">
         * @throws java.io.IOException
         * @throws java.lang.InterruptedException
     */
            // Compile all the words using regex
            Pattern regex = Pattern.compile("\\w+");
            Matcher m = regex.matcher(value.toString());

            // Get the name of the file from the inputsplit in the context
            String docName =((FileSplit) context.getInputSplit()).getPath().getName();

            while (m.find()) 
            {
                String matchedKey = m.group().toLowerCase();
                //if first letter is a character then is a word
                if(Character.isLetter(matchedKey.charAt(0)))
                {
                    Text  term_docKey = new Text(matchedKey+","+docName);
                    context.write(term_docKey, new IntWritable(1));
                }
            }
        }
    }  

public static class TCDocReducer  extends Reducer<Text, IntWritable, Text, IntWritable> 
{
    /**
     * @param key is the key of the mapper
     * @param values are all the values aggregated during the mapping phase
     * @param context contains the context of the job run
     *
     *             PRECONDITION: receive  <word,doc>, 1,1,1,1,1,1">
     *             POSTCONDITION: (<word,doc>,sum)
         * @throws java.io.IOException
         * @throws java.lang.InterruptedException
     */

    //Declare a intwritable to save the sum of term occurances in doc
    private final  IntWritable result = new IntWritable();
     @Override
   public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
   {

      //initialize sum variable that holds the number of term occurances in doc
       int sum = 0;
        for (IntWritable val : values) {
            //add all  1 for the key and save it to sum
            sum += val.get();
        }
        //set sum value to result IntWritable    
        result.set(sum);
        //Write (<term,doc>,sum) to output file
        context.write(key, result);
  
    }
}

}
