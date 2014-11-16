package sparse

import org.apache.spark
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.util.Random

object SparseUtility {
    // Generate vectors with normal distributed entries for testing 
    def randomVector(mean: Double, variance: Double, length: Long): Vector = {
        val generator:Random = new Random()
        val output = for( i <- 0 until length.toInt) yield mean + generator.nextGaussian() * variance   
        Vectors.dense(output.toArray)
    }   
    
    // Extract this routine as a separate function for future usage
    def transform(vectorArray: Array[(Long, Double)], length: Int): Vector = {
        val result = new Array[Double](length)
        for (i <- 0 until vectorArray.length){
            result(vectorArray(i)._1.toInt) += vectorArray(i)._2
        }   
        Vectors.dense(result)
    }
}