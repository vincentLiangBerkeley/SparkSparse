package sparse

import breeze.linalg.{DenseMatrix => BDM}

import org.apache.spark
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import org.apache.spark.mllib.linalg.distributed._

/**
 * :: Experimental ::
 * Represents an entry in an distributed matrix.
 * @param i row index
 * @param j column index
 * @param value value of the entry
 */
@Experimental
case class MatrixEntry(i: Long, j: Long, value: Double)

/**
 * :: Experimental ::
 * Represents a matrix in coordinate format.
 *
 * @param E type parameter, could be Double, Int Boolean
 * @param entries matrix entries
 * @param nRows number of rows. A non-positive value means unknown, and then the number of rows will
 *              be determined by the max row index plus one.
 * @param nCols number of columns. A non-positive value means unknown, and then the number of
 *              columns will be determined by the max column index plus one.
 * @param sym A boolean parameter telling us whether the matrix is symmetric, if it is, only "triu" will be stored
 */
@Experimental
class CoordinateMatrix(
    val entries: RDD[MatrixEntry],
    private var nRows: Long,
    private var nCols: Long,
    private val sym: Boolean = false) extends DistributedMatrix {

  /** Alternative constructor leaving matrix dimensions to be determined automatically. */
  def this(entries: RDD[MatrixEntry]) = this(entries, 0L, 0L)

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

  // This contains calls to a method in SparseUtility, where the real multiplication routine is stored
  def multiply(vector: Vector, sc: SparkContext, trans: Boolean = false, numTasks: Int = 4): Vector = {
    if (!sym) multiplyHelper(this, vector, sc, trans, numTasks)
    else {
      // Notice that a symmetric matrix must be square
      // So we compute Ay + (A - diag(A))^Ty
      val first = multiplyHelper(this, vector, sc, false, numTasks)

      // This is potentially inefficient because it requires space for new coordinate matrix
      val lowerA = new CoordinateMatrix(this.entries.filter(entry => entry.i != entry.j), numRows, numCols)

      val second = multiplyHelper(lowerA, vector, sc, true, numTasks)

      val result = for( i <- 0 until first.size) yield first(i) + second(i)
      Vectors.dense(result.toArray)
    }
  }

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
  private def multiplyHelper(matrix: CoordinateMatrix, vector: Vector, sc: SparkContext, trans: Boolean = false, numTasks: Int = 4): Vector = {
        if (matrix.numRows > Int.MaxValue || matrix.numCols > Int.MaxValue)
            sys.error("Cannot multiply this matrix because size is too large!")

        // Length of the output vector
        val length = if (trans) matrix.numCols.toInt else matrix.numRows.toInt
        require(vector.size == matrix.numCols.toInt, "Matrix vector size mismatch!")

        val copies = sc.broadcast(vector.toArray)
        // This is a RDD of MatrixEntry
        val entries = matrix.entries.map{entry =>
             if(trans) (entry.j, (entry.i, entry.value))
             else (entry.i, (entry.j, entry.value))
        }.cache()

        System.out.println("Num of partitions originally is " + entries.partitions.size)

        // Map each row with the vector entry
        // Repartition will be used here so that reduceByKey will communicate minimal information
        val mappedMatrix = entries.map{ entry => 
            val index = entry._2._1
            val value = copies.value(index.toInt) * entry._2._2
            (entry._1, value)
        }
        //.partitionBy(new spark.HashPartitioner(length / 5)).persist

        System.out.println("Num of partitions after repartition is " + mappedMatrix.partitions.size)

        val vectorArray = mappedMatrix.reduceByKey(_+_, numTasks).collect // The number of tasks could be changing
        
        SparseUtility.transform(vectorArray, length)  
      }

  /** Converts to IndexedRowMatrix. The number of columns must be within the integer range. */
  def toIndexedRowMatrix(): IndexedRowMatrix = {
    val nl = numCols()
    if (nl > Int.MaxValue) {
      sys.error("Cannot convert to a row-oriented format because the number of columns $nl is " +
        "too large.")
    }
    val n = nl.toInt

    // We have to cast all the entries into doubles again
    val entriesAsDoubles: RDD[(Long, Int, Double)] = entries.map {entry =>
      (entry.i, entry.j.toInt, entry.value)
    }

    val indexedRows = entriesAsDoubles.map(entry => (entry._1, (entry._2, entry._3)))
    .groupByKey().map { 
      case (i, vectorEntries) => IndexedRow(i, Vectors.sparse(n, vectorEntries.toSeq))
    }

    new IndexedRowMatrix(indexedRows, numRows(), n)
  }

  /**
   * Converts to RowMatrix, dropping row indices after grouping by row index.
   * The number of columns must be within the integer range.
   */
  def toRowMatrix(): RowMatrix = {
    this.toIndexedRowMatrix().toRowMatrix()
  }

  /** Determines the size by computing the max row/column index. */
  private def computeSize() {
    // Reduce will throw an exception if `entries` is empty.
    val (m1, n1) = entries.map(entry => (entry.i, entry.j)).reduce { case ((i1, j1), (i2, j2)) =>
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
    entries.collect().foreach { case MatrixEntry(i, j, value) =>
      mat(i.toInt, j.toInt) = value
      if (sym) mat(j.toInt, i.toInt) = value
    }
    mat
  }
}