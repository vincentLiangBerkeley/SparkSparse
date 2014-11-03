package sparse

import breeze.linalg.{DenseMatrix => BDM}

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
case class MatrixEntry[E](i: Long, j: Long, value: E)

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
 */
@Experimental
class CoordinateMatrix[E](
    val entries: RDD[MatrixEntry[E]],
    private var nRows: Long,
    private var nCols: Long) extends DistributedMatrix {

  /** Alternative constructor leaving matrix dimensions to be determined automatically. */
  def this(entries: RDD[MatrixEntry[E]]) = this(entries, 0L, 0L)

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

   /**
    Multiplies a Coordinate matrix with a local vector.
    @param matrix: The coordinate matrix to be multiplied with
    @Param vector: The local vector, could be multiplied on the left or right
    @Param sc: The SparkContext that handles parallel computations
    @Param(Optional) numTasks: THe number of tasks used to reduce the result, default is 4
    @Param(Optional) "trans": A boolean variable indicating the "trans" of multiplication, by default is "true", meaning on the right
    x = A * v (if trans = true)
    x = A' * v (otherwise)
  */
  def multiply(vector: Vector, sc: SparkContext, numTasks: Int = 4, trans: Boolean = true): Vector = {
    if (trans) require(vector.size == this.numCols.toInt, "Matrix vector size mismatch!")
    else require(vector.size == this.numRows.toInt, "Matrix vector size mismatch!")

    val copies = sc.broadcast(vector.toArray)
    // This is a RDD of MatrixEntry
    val entries = this.entries

    // Map each row with the vector entry
    val mappedMatrix = entries.map{ entry => 
        // @Problem: Possibility is that the matrix is too large, j.toInt overflows
        val index = if(trans) entry.j else entry.i
        val value = entry.value match {
            case bv: Boolean => copies.value(index.toInt)
            case iv: Int => iv * copies.value(index.toInt)
            case dv: Double => dv * copies.value(index.toInt)
        }
        if (trans) (entry.i, value) else (entry.j, value)
    }

    val vectorArray = mappedMatrix.reduceByKey(_+_, 4).collect // The number of tasks could be changing
    val length = if (trans) this.numCols.toInt else this.numRows.toInt
    SparseUtility.transform(vectorArray, length)  
  }

  // I do not allow transformations to indexed-row matrices at the moment

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
      entry.value match {
        case bv: Boolean => (entry.i, entry.j.toInt, 1.00)
        case iv: Int => (entry.i, entry.j.toInt, iv.toDouble)
        case db: Double => (entry.i, entry.j.toInt, db)
      }
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
      value match {
        case bv: Boolean => mat(i.toInt, j.toInt) = 1.00
        case iv: Int => mat(i.toInt, j.toInt) = iv.toDouble
        case v: Double => mat(i.toInt, j.toInt) = v
      }
    }
    mat
  }
}