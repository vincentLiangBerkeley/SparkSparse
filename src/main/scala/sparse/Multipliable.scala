package sparse
    
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.distributed.DistributedMatrix
import breeze.linalg.{DenseMatrix => BDM}    


trait Multipliable extends DistributedMatrix{
  def multiply(vector: Vector, sc: SparkContext, trans: Boolean = false): LongVector
  val entries: RDD[MatrixEntry] 
  protected val sym: Boolean

  private[sparse] def toBreezeMat(): BDM[Double] = {
    val m = numRows().toInt
    val n = numCols().toInt
    val mat = BDM.zeros[Double](m, n)
    entries.collect().foreach { case MatrixEntry(i, j, value) =>
      mat(i.toInt, j.toInt) = value
      if (sym) mat(j.toInt, i.toInt) = value
    }
    mat
  }
    
  protected def checkSize(matSize: Long, vecSize: Int) = {
    require(matSize < Int.MaxValue , "Cannot multiply because the matrix is too large!")
    require(matSize.toInt == vecSize, "Matrix vector size mismatch!")
  }
}