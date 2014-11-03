package sparse

import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, CoordinateMatrix}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.util.Random

object SparseUtility {
    /*
    Multiplies a Coordinate matrix with a local vector.
    @Param matrix: The coordinate matrix to be multiplied with
    @Param vector: The local vector, could be multiplied on the left or right
    @Param sc: The SparkContext that handles parallel computations
    @Param(Optional) numTasks: THe number of tasks used to reduce the result, default is 4
    @Param(Optional) "trans": A boolean variable indicating the "trans" of multiplication, by default is "true", meaning on the right

    x = A * v (if trans = true)
    x = A' * v (otherwise)
     */
    def multiply(matrix: CoordinateMatrix, vector: Vector, sc: SparkContext, numTasks: Int = 4, trans: Boolean = true): Vector = {
        //@ Problem: What if matrix is too huge that toInt overflows?
        if (trans) require(vector.size == matrix.numCols.toInt, "Matrix vector size mismatch!")
        else require(vector.size == matrix.numRows.toInt, "Matrix vector size mismatch!")

        val copies = sc.broadcast(vector.toArray)
        // This is a RDD of MatrixEntry
        val entries = matrix.entries

        // Map each row with the vector entry
        val mappedMatrix = entries.map{ entry => 
            // @Problem: Possibility is that the matrix is too large, j.toInt overflows
            val index = if(trans) entry.j else entry.i
            val value = entry.value * copies.value(index.toInt)
            if (trans) (entry.i, value) else (entry.j, value)
        }

        val vectorArray = mappedMatrix.reduceByKey(_+_, 4).collect // The number of tasks could be changing
        val length = if (trans) matrix.numCols.toInt else matrix.numRows.toInt
        transform(vectorArray, length)  
    }

    // Generate vectors with normal distributed entries for testing 
    def randomVector(mean: Double, variance: Double, length: Long): Vector = {
        val generator:Random = new Random()
        val output = for( i <- 0 until length.toInt) yield mean + generator.nextGaussian() * variance

        Vectors.dense(output.toArray)
    }

    // Extract this routine as a separate function for future usage
    private def transform(vectorArray: Array[(Long, Double)], length: Int): Vector = {
        val result = new Array[Double](length)
        var i = 0
        for (i <- 0 until vectorArray.length){
            result(vectorArray(i)._1.toInt) = vectorArray(i)._2
        }

        Vectors.dense(result)
    }
}