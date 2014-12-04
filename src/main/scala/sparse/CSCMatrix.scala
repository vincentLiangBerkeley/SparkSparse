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


@Experimental
class CSCMatrix(
    val entries: RDD[MatrixEntry],
    private var nRows: Long,
    private var nCols: Long,
    protected val sym: Boolean = false, 
    private val partNum: Int = 4) extends Multipliable {
    
    def this(entries: RDD[MatrixEntry]) = this(entries, 0L, 0L)
    private val rowForm = toLocalCSC(entries.map(entry => (entry.i, (entry.j.toInt, entry.value))), false).persist
    private val colForm = toLocalCSC(entries.map(entry => (entry.j, (entry.i.toInt, entry.value))), true).persist

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

    def multiply(vector: Vector, sc: SparkContext, trans: Boolean = false): LongVector = {
        // Check for sizes of multiplication
        if(trans) require(vector.size == numRows.toInt, "Matrix-vector size mismatch!")
        else require(vector.size == numCols.toInt, "Matrix-vector size mismatch!")

        val copies = sc.broadcast(vector.toArray)
        val v = DenseVector(copies.value)

        if(!sym){
            val matrix = if(trans) colForm else rowForm

            val result = matrix.flatMap{
            case(arr, mat) =>
                val partialVec = mat * v
                arr zip partialVec.toArray
            }
            new LongVector(result)
        }else{
            // This is inefficient because we have to do the communication twice
            // Question: Can we do this only once?
            val first = rowForm.flatMap{
                case (arr, mat) => 
                    var partialVec = mat * v
                    for( i <- 0 until arr.length) {
                        partialVec(i) = partialVec(i) - mat(i, arr(i).toInt) * v(arr(i).toInt)
                    }
                    arr zip partialVec.toArray
            }
            val second = colForm.flatMap{
             case(arr, mat) =>
                val partialVec = mat * v
                arr zip partialVec.toArray
            }

            val result = (first join second).mapValues{case (v1, v2) => v1 + v2}

            new LongVector(result)
        }      
    }

    //private def mapMultiply(matrix: RDD[(Array[Double], BCM[Double])], v: Array[Double]): RDD[(Long, Double)]


    // Internally it stores as an RDD of (rows, BCM)
    // Partitioner here needs to be careful
    private def toLocalCSC(entries: RDD[(Long, (Int, Double))], trans: Boolean): RDD[(Array[Long], BCM[Double])] = {
        val minorSize = if(trans) numRows.toInt else numCols.toInt
        entries.groupByKey(new spark.HashPartitioner(partNum)).mapPartitionsWithIndex{
            case (ind, iter) => 
                // Doing size = iter.size will cause the iterator to iterate to the end and lose all information
                val allRows = iter.toArray
                val size = allRows.size
                val builder = new BCM.Builder[Double](size, minorSize)
                val rowIndeces = new Array[Long](size)
                for(i <- 0 until size) {
                    val row = allRows(i) // This is a tuple (Long, Seq[(Int, Double)])
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