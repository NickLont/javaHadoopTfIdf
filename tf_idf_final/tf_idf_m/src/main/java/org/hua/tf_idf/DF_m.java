
package org.hua.tf_idf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public  class DF_m {
      
    public static class TermInCorpusMapper extends Mapper<Object, Text, Text, Text> {
    public TermInCorpusMapper() {
    }
 
    @Override
    public void map(Object key, Text value,Context context) throws IOException, InterruptedException {
      //Read the input file line by line  and split on space character, to seperate key<term,doc> from  sum value
        String[] inputstring = value.toString().split("\t");
        
        //assign the  key<term,doc> to termdockey variable
        String termdockey=inputstring[0];
         //assign the value input string to sum variable
        String sum=inputstring[1];
        
        //split the key to seperate the term from the  docname
        String[] termdoc =termdockey.split(",");
        //assign term value to term variable
        String term=termdoc[0];
         //assign docname to doc variable
        String doc=termdoc[1];
       
        context.write(new Text(term+",dummy"), new Text(doc));
    }
    }
    
    
    public  static class TermInCorpusReducer extends Reducer<Text, Text, Text, IntWritable> {
 
   // private  final DecimalFormat DF = new DecimalFormat("###.########");
 
    public TermInCorpusReducer() {
    }
    /**
     * @param key is the key of the mapper
     * @param values are all the values aggregated during the mapping phase
     * @param context contains the context of the job run
     *
     *             PRECONDITION: receive  <word,dummy>, <doc1,doc2,doc3,doc4,doc5>>
     *             POSTCONDITION: (<word,dummy>,df)
         * @throws java.io.IOException
         * @throws java.lang.InterruptedException
     */
       
    private final  IntWritable df = new IntWritable();
     //private final  int numberOfDocumentsInCorpusWhereKeyAppears = new LongWritable();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    // String DocsNum= context.getConfiguration().get("docsInCorpus");
    
    //Initialize sum
        int sum = 0;
        //Iterate though the list of documents and addd 1 for every document
        for (Text val : values) {
         //
            sum++;
           
        }
        df.set(sum);
       
    //Write to contect <word,dummy,df>
          context.write( key,  df);
    
    }
    }
    
}
