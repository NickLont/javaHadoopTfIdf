/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hua.tf_idf;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
//import java.util.HashMap;
//import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;




public class tf_idf_m_final {

    public static class Tf_idf_mMapper extends Mapper<LongWritable, Text, Text, Text> {
    public Tf_idf_mMapper() {
    }
     @Override
    public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException { 
         

        /**
           * Receives two kinds of input (<word,doc>,tf>) and (<word,dummy>,df)
           *  writes to context (<word>,[inputdirectory],doc,tf) or (<word>,[inputdirectory],dummy,df)
           */
        
     String inputdirectory =((FileSplit) context.getInputSplit()).getPath().toString();
                
    //Split on space to separete key from value from the input text
     String[] wordAndDocOrDummyPlusResult = value.toString().split("\t");
     
     //Get the tf or df Value
     String tfOrDf=wordAndDocOrDummyPlusResult[1];

     //Get the word,doc or word,dummy
     String [] wordAndDocOrDummy= wordAndDocOrDummyPlusResult[0].split(",");
     
     //get the word
      String word=wordAndDocOrDummy[0];
      
    //get doc or dummy depending which input we are reading-results from tf or results from df
     String DocOrDummy=wordAndDocOrDummy[1];
     
     //Create the new value to write to context
       String newcontext=inputdirectory+","+DocOrDummy+","+tfOrDf;
       
       //Write to contect (<word>,doc,tf) or (<word>,dummy,df)
        context.write( new Text(word), new Text(newcontext));
    }
    }
    
    
    
    
    
     public  static class Tf_idf_mReducer extends Reducer<Text, Text, Text, Text>  {
 
    private  final DecimalFormat DF = new DecimalFormat("###.########");
    private   String dfstring; 
    public Tf_idf_mReducer() {
    }
    /**
     * @param key is the key of the mapper
     * @param values are all the values aggregated during the mapping phase
     * @param context contains the context of the job run
     *
     *             PRECONDITION: receive a list of(<word>,([inputdirectory],doc1,tf),(inputdirectory],doc2,tf),....[inputdirectory],dummy,df))
     *             POSTCONDITION:(<word,doc1>,idf,df,tf)
         * @throws java.io.IOException
         * @throws java.lang.InterruptedException
     */
   
    
   @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    
         
     //Create a map to store the <docnanme,tf> of the Iterable<Text> values 
     Map<String, String> tempDocTf = new HashMap<String, String>();
    
     //Read number of documents in Corpus passed to the configuration
    String DocsNumString= context.getConfiguration().get("docsInCorpus");
    //Parse to int
    int DocsNum=Integer.parseInt(DocsNumString);
    //Boolean to check if the <dummy,df> string found in the values
    
    //Read Configuration to find the output destination of DF
    String DFoutput= context.getConfiguration().get("jobdfOutput");
 
    //Iterate through values (doc1,tf),(doc2,tf),....(dummy,df)
    for (Text val : values) 
    {
        ///Check if the 1st value contains the DFoutput folder
       if(val.toString().split(",")[0].contains(DFoutput))
       {
           //Second values is df
            dfstring =val.toString().split(",")[2];
            //Update DfStringFound boolean
           // DfStringFound=true;
       }
       else
       {
            //It is Text<"..outputTC",doc,tf>
            //Split the Text values to get the doc and tf
             String DocName=val.toString().split(",")[1];
            String tf=val.toString().split(",")[2];
            //Save the doc,tf in the Map
            tempDocTf.put(DocName, tf);
       }
           
    }

   
            //Parse df to int
            int df = Integer.parseInt(dfstring);
            //Compute idf
            double idf = (double) DocsNum  / (double) df;
            
            //Iterate through the Map holding doc,tf values
            for (String document : tempDocTf.keySet()) {
                        //Read tf
                        String TfString = tempDocTf.get(document);
                        int tf= Integer.parseInt(TfString);
                        //Calculate tfidf :w =log10(1+tf)* log(10)(N/df)
                        double tfIdf =Math.log10(1+tf)* Math.log10(idf);
                        //Cretea string containing idf,df,tf values
                        String v="tfidf="+DF.format(tfIdf)+",df="+dfstring+",tf="+TfString;
                        //Write to context the final results!!
                         context.write( new Text(key+","+document),  new Text(v));
                }
    
    
    }
    }
    
    
}
