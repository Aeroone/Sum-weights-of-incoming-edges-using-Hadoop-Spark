import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Task2 {
  def main(args: Array[String]) 
  {
    val sc = new SparkContext(new SparkConf().setAppName("Task2"))
    
    // read the file
    val file = sc.textFile("hdfs://localhost:8020" + args(0))
	
	/* TODO: Needs to be implemented */
	//split into (source node, target node, weight)
	val line = file.map(_.split("\t"))
	
	//map to (node, weight) 
	val Node_weight_pairs = line.map( x=>(x(1),x(2).toInt) )
		
	//reduce
	val Sum_pairs = Node_weight_pairs.reduceByKey(_+_)
	
	//filter
	val Filtered_Sum_pairs = Sum_pairs.filter(_._2>0) 
		
	//output form
	val Output_pairs = Filtered_Sum_pairs.map( x=>(x._1+"\t"+x._2) )
										 	
	// store output on given HDFS path.
    // YOU NEED TO CHANGE THIS
    Output_pairs.saveAsTextFile("hdfs://localhost:8020"+args(1))
  }
}
