package sparse

import breeze.linalg.{CSCMatrix => BCM}
import breeze.linalg.{DenseMatrix => BDM}
import breeze.linalg.DenseVector

import org.apache.spark
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import org.apache.spark.mllib.linalg.distributed._


@Experimental
// TODO: This class does not support symmetric matrix now, should add support
class CSRMatrix(
    val entries: RDD[(Long, Long, Double)],
    private var nRows: Long,
    private var nCols: Long,
    private val sym: Boolean = false, 
    private val partNum: Int = 4) extends DistributedMatrix {
    
    def this(entries: RDD[(Long, Long, Double)]) = this(entries, 0L, 0L)
    val localCSC = toLocalCSC(entries.map{case(i, j, value) => (i, (j.toInt, value))}).persist

    /** Gets or computes the number of columns. */
    override def numCols(): Long = {
      if (nCols <= 0L) {
        computeSize()
      }
      nCols
    }
  
    /** Gets or computes the number of rows. */
    override def numRows(): Long = {
      if (nRows <= 0L) {
        computeSize()
      }
      nRows
    }

    def multiply(vector: Vector, sc: SparkContext, trans: Boolean = false, numTasks: Int = partNum): Vector = {
        val copies = sc.broadcast(vector.toArray)
        val v = DenseVector(copies.value)

        val result = localCSC.map{
            case(arr, mat) =>
                val partialVec = (mat * v)
                arr zip partialVec.toArray
        }.collect.flatten

        SparseUtility.transform(result, numRows.toInt)
    }

    // Internally it stores as an RDD of (rows, BCM)
    private def toLocalCSC(entries: RDD[(Long, (Int, Double))]) = {
        entries.groupByKey(new spark.HashPartitioner(partNum)).mapPartitionsWithIndex{
            case (ind, iter) => 
                // Doing size = iter.size will cause the iterator to iterate to the end and lose all information
                val allRows = iter.toArray
                val size = allRows.size
                val builder = new BCM.Builder[Double](size, numCols.toInt)
                val rowIndeces = new Array[Long](size)
                for(i <- 0 until size) {
                    val row = allRows(i) // This is a tuple (Long, Seq[(Int, Double)])
                    //System.out.println("Current row index is " + row._1 + " and current partition index is " + ind 
                    //    + " so the local index should be " + localRowIndex)
                    rowIndeces(i) = row._1 // Record the row index
                    val rowEntries = row._2.toArray
                    for( j <- 0 until rowEntries.size) {
                        // entry: (Int, Double)
                        val entry = rowEntries(j)
                        builder.add(i, entry._1, entry._2)
                    }
                }
                val localMatrix = builder.result
            // Give back the iterator of a single BCM matrix
            Iterator((rowIndeces, localMatrix))
        }
    }

    /** Determines the size by computing the max row/column index. */
    private def computeSize() {
        // Reduce will throw an exception if `entries` is empty.
        val (m1, n1) = entries.map(entry => (entry._1, entry._2)).reduce { case ((i1, j1), (i2, j2)) =>
          (math.max(i1, i2), math.max(j1, j2))
        }
        // There may be empty columns at the very right and empty rows at the very bottom.
        nRows = math.max(nRows, m1 + 1L)
        nCols = math.max(nCols, n1 + 1L)
    }

    /**
    * Also make this toBreeze public because it has to be at least private to mllib, which I cannot
    * restrict to.
    */
    override def toBreeze(): BDM[Double] = {
      val m = numRows().toInt
      val n = numCols().toInt
      val mat = BDM.zeros[Double](m, n)
      entries.collect().foreach { case(i, j, value) =>
        mat(i.toInt, j.toInt) = value
        if (sym) mat(j.toInt, i.toInt) = value
      }
      mat
    }
}