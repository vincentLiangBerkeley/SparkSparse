package sparse

import breeze.linalg.{DenseMatrix => BDM}

import org.apache.spark.graphx._
import org.apache.spark._
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vectors,Vector}

@Experimental
/**
 * This is a very primary experimental implementation of Graph matrix
 * numRows, numCols will be added to the class later on
 * @type {[type]}
 */
class GraphMatrix(
    val entries: RDD[MatrixEntry],
    val numRows: Long,
    val numCols: Long,
    protected val sym: Boolean, 
    private val partNum: Int
    ) extends Multipliable {
    
    // Currently only support square matrices
    require(numRows == numCols, "Matrix is not square!")

    // Vertices represent the rows
    private val vertices: RDD[(VertexId, Double)] = entries.map(entry => (entry.i, 0.0))
    // Edges represent nonzero entries
    // We can partition the edges so that edges with the same srcId are together
    private val edges: RDD[Edge[Double]] = entries.map(entry => (entry.i, (entry.j, entry.value)))
                                            .partitionBy(new HashPartitioner(partNum))
                                            .map{case(key, value) => new Edge(key, value._1, value._2)}
                                            .cache()

    val graphMatrix: Graph[Double, Double] = Graph(vertices, edges).cache()

    def multiply(vector: Vector, sc: SparkContext, trans: Boolean = false): LongVector = {
        val copies = sc.broadcast(vector.toArray)
        val multiplied: VertexRDD[Double] = graphMatrix.mapReduceTriplets[Double](
            triplet => { // Map function
                Iterator((triplet.srcId, triplet.attr * copies.value(triplet.dstId.toInt)))
            },
            (a, b) => a + b // Reduce function is to sum up the mapped edge values
        )

        val result = multiplied
        new LongVector(result, numRows)
    }

    def toBreeze(): BDM[Double] = {
      val m = numRows.toInt
      val n = numCols.toInt
      val mat = BDM.zeros[Double](m, n)
      entries.collect().foreach (entry =>
        mat(entry.i.toInt, entry.j.toInt) = entry.value
        //if (sym) mat(j.toInt, i.toInt) = value
      )
      mat
    }
}