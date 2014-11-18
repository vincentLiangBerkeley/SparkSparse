package sparse

import breeze.linalg.{CSCMatrix => BCM}

import org.apache.spark
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import org.apache.spark.mllib.linalg.distributed._}


@Experimental
class CSRMatrix(
    val entries: RDD[(Long, Long, Double)],
    private var rows: Long,
    private var cols: Long,
    private val sym: Boolean = false, 
    private val partNum: Int = 4) extends DistributedMatrix {
    
    def this(entries: RDD[(Long, Long, Double)]) = this(entries, 0L, 0L)

    // Internally it stores as an RDD of (rows, BCM)
    private def toLocalCSC(entries: RDD[(Long, (Int, Double))]) = {
        entries.groupByKey(new spark.HashPartitioner(partNum)).mapPartitionsWithIndex{
            case (ind, iter) => 
                val size = iter.size
                val builder = new BCM.Builder[Double](size, cols.toInt)
                val rowIndeces = Array.empty[Int]
                // Iterate over the rows
                while(iter.hasNext){
                    val row = iter.next // This is a tuple (rowInd, iterator)
                    val localRowIndex = (row._1 - ind) / partNum + 1 // Convert the row index to local
                    rowIndeces:+ row._1
                    while(row._2.hasNext){
                        val entry = row._2.next
                        builder.add(localRowIndex, entry._1, entry._2)
                    }
                }
                val localMatrix = builder.result
            // Give back the iterator of a single BCM matrix
            Iterator((rowIndeces, localMatrix))
        }
    }
}