package org.hua.tf_idf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;




public class Tf_idf_m{

   
    public static void main(String[] args) throws Exception {
   
        //Check if  All 3(inputpath,outpath and docsinCopurs) arguments are passed
      if (args.length<3)
      {
           System.out.println("Error: Missing argumnets.Yous should provide 3 arguments: inputfolderpath outputfolderpath numberofdocs");
           System.exit(-3);  
      }
      else if(args.length>3)
      {
      
            System.out.println("Error: Too many argumnets.Yous should provide 3 arguments: inputfolderpath outputfolderpath numberofdocs");
            System.exit(-2);  
      }
      //Try convert the 3rd argument to int
        try
        { Integer.parseInt(args[2]);
        }
        //If convertion to int failed-exit -1:No job started
        catch (NumberFormatException  e) 
         {
              System.out.println("Error:Third argument must be a valid number, representing the number of docs.Details:'"+e+"'");
              System.exit(-1);  
         }
         
        //Pass number of documents int the configuration
        Configuration conf = new Configuration();//getConf();
        conf.set("docsInCorpus", args[2]);
         
        //Start a job to Compute tf
        Job jobtf = Job.getInstance(conf, "Compute term count in  Doc");

        
        jobtf.setJarByClass(Tf_m.class);
        jobtf.setMapperClass(Tf_m.TCDocMapper.class);
        jobtf.setReducerClass(Tf_m.TCDocReducer.class);     
        
        jobtf.setOutputKeyClass(Text.class);
        jobtf.setOutputValueClass(IntWritable.class);
        //Read from the path passed as first argument
        FileInputFormat.addInputPath(jobtf, new Path(args[0]));
        //Write the Results in output folder 
        FileOutputFormat.setOutputPath(jobtf, new Path( "hdfs://localhost:54310/tfidf_m/outputTF"));
        //Check Job completed=0
          int tf=jobtf.waitForCompletion(true) ? 0 : 1;
          
 /////////////////////////////////////////////////df///////////////////////////////////
  if(tf==0)
    {
            //Start job to calculate df
             Job jobdf = Job.getInstance(conf, "Document Frequency, DF");
 
            jobdf.setJarByClass(DF_m.class);
           jobdf.setMapperClass(DF_m.TermInCorpusMapper.class);
            jobdf.setReducerClass(DF_m.TermInCorpusReducer.class);
 
           jobdf.setOutputKeyClass(Text.class);
           jobdf.setOutputValueClass(Text.class);
            //Read the results from the previous job output, so tha we get distinct filenames for every word!!
            FileInputFormat.addInputPath(jobdf, new Path("hdfs://localhost:54310/tfidf_m/outputTF"));
            //Write results to dfs folder
            FileOutputFormat.setOutputPath(jobdf, new Path("hdfs://localhost:54310/tfidf_m/outputDF"));
        int df=  jobdf.waitForCompletion(true) ? 0 : 1; 
////////////////////////df_end////////////////////////////////////////////////////////////////
 //////////////////tfifdf//////////////////////////////////////////////////////////////////
            //Check If previous job completed succesfully    
            if(df==0)
            {
              conf.set("jobdfOutput", "outputDF");  
                
              Job jobtfidf = Job.getInstance(conf, "Compute TFIDF in Corpus, TF-IDF");
    
            jobtfidf.setJarByClass(Tf_idf_m.class);
           jobtfidf.setMapperClass(tf_idf_m_final.Tf_idf_mMapper.class);
            jobtfidf.setReducerClass(tf_idf_m_final.Tf_idf_mReducer.class);
 
           jobtfidf.setOutputKeyClass(Text.class);
           jobtfidf.setOutputValueClass(Text.class);
            //Read the results of the 1st job
            FileInputFormat.addInputPath(jobtfidf, new Path("hdfs://localhost:54310/tfidf_m/outputDF"));
            //Read the results of the 2nd job
            FileInputFormat.addInputPath(jobtfidf, new Path("hdfs://localhost:54310/tfidf_m/outputTF"));
              
            //Write the results to the path passed as 2nd argument
            FileOutputFormat.setOutputPath(jobtfidf, new Path(args[1]));
                        //Exit with code 3:3r job failed or 0:success
            System.exit(jobtfidf.waitForCompletion(true) ? 0 : 3);
            }
         }

       
        else
         {
             //Exit with code 2-2nd job failed
               System.exit(2);
         }
  
  }

     

}