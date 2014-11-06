package sparse

import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.util.Random

object SparseUtility {
    /**
    Multiplies a Coordinate matrix with a local vector.
    @param matrix: The coordinate matrix to be multiplied with
    @param vector: The local vector, could be multiplied on the left or right
    @param sc: The SparkContext that handles parallel computations
    @param(Optional) numTasks: THe number of tasks used to reduce the result, default is 4
    @Param(Optional) "trans": A boolean variable indicating the "trans" of multiplication, by default is "false", meaning on the right
    x = A * v (if trans = false)
    x = A' * v (if trans = true)
  */
def multiplyHelper[E](matrix: CoordinateMatrix[E], vector: Vector, sc: SparkContext, trans: Boolean = false, numTasks: Int = 4): Vector = {
    if (trans) require(vector.size == matrix.numCols.toInt, "Matrix vector size mismatch!")
    else require(vector.size == matrix.numRows.toInt, "Matrix vector size mismatch!")

    val copies = sc.broadcast(vector.toArray)
    // This is a RDD of MatrixEntry
    val entries = matrix.entries

    // Map each row with the vector entry
    val mappedMatrix = entries.map{ entry => 
        // @Problem: Possibility is that the matrix is too large, j.toInt overflows
        val index = if(trans) entry.i else entry.j
        val value = entry.value match {
            case bv: Boolean => copies.value(index.toInt)
            case iv: Int => iv * copies.value(index.toInt)
            case dv: Double => dv * copies.value(index.toInt)
        }
        if (trans) (entry.j, value) else (entry.i, value)
    }

    val vectorArray = mappedMatrix.reduceByKey(_+_, numTasks).collect // The number of tasks could be changing
    val length = if (trans) matrix.numCols.toInt else matrix.numRows.toInt
    SparseUtility.transform(vectorArray, length)  
  }
    // Generate vectors with normal distributed entries for testing 
    def randomVector(mean: Double, variance: Double, length: Long): Vector = {
        val generator:Random = new Random()
        val output = for( i <- 0 until length.toInt) yield mean + generator.nextGaussian() * variance

        Vectors.dense(output.toArray)
    }

    // Extract this routine as a separate function for future usage
    def transform(vectorArray: Array[(Long, Double)], length: Int): Vector = {
        val result = new Array[Double](length)
        var i = 0
        for (i <- 0 until vectorArray.length){
            result(vectorArray(i)._1.toInt) = vectorArray(i)._2
        }

        Vectors.dense(result)
    }
}