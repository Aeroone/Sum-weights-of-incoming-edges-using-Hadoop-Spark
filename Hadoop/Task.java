import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class Task1 
{
 
  public static class GraphMapper
       extends Mapper<Object, Text, Text, IntWritable>
  {

		//Define the node and its weight
		private Text node = new Text();
        private IntWritable weight = new IntWritable();
				
		public void map(Object key, Text value, Context context
						) throws IOException, InterruptedException 
		{
		  
		  //Obtain each node and weight pair 
		  StringTokenizer itr = new StringTokenizer(value.toString(),"\t");
		  String[] strVector = new String[itr.countTokens()];
		  
		  int i=0;
		  while (itr.hasMoreTokens()) 
		  {
			strVector[i]=itr.nextToken();  
			i++; 
		  }			  
		 
		  //Set the key_value pair
		  node.set( strVector[1] ) ;
		  weight.set( Integer.parseInt( strVector[2] ) );  
		  
		  context.write(node, weight);  
		  		  
		}
  }

  public static class GraphReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> 
  {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException 
	{
      int sum = 0;
      for (IntWritable val : values) 
	  {
        sum += val.get();
      }
      
	  //Filter the results
	  if (sum!=0)
	  {
		result.set(sum);
		context.write(key, result);  
	  }
	
	}
  }

  
  public static void main(String[] args) throws Exception 
  {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Task1");

    /* TODO: Needs to be implemented */
	job.setJarByClass(Task1.class);
	
	//Map-Combine-Reduce
	job.setMapperClass(GraphMapper.class);
    job.setCombinerClass(GraphReducer.class);
    job.setReducerClass(GraphReducer.class);
    
	//Output Class
	job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
	
	//Input and Output	
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
