package sparse

import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.{Vectors, Vector}

class LongVector(val entries: RDD[(Long, Double)], val length: Long) {
    // Values are calculated when you initialize the instance of the class
    val activeSize: Long = entries.count

    /**
     * The array format of the vector if it is short(< Int.MaxValue)
     * @type {Array[Double]}
     */
    val arr: Array[Double] = {
        if (length > Int.MaxValue) Array[Double]()
        else SparseUtility.transform(entries.collect, length.toInt)
    }

    /**
     * Transform the vector into array, throws exception if length is too large
     * @type {RDD[(Long, Double) => Array[Double]}
     */
    def toArray: Array[Double] = {
        if (arr.size == 0) throw new UnsupportedOperationException("Cannot transform to a local vector because the length is too large!")
        else arr
    }

    /**
     * Try to collect the entries.
     * @type {RDD[(Long, Double) => Unit}
     */
    def collect: Unit = 
        try { 
          entries.collect
        } catch {
          case e: Exception => 
            System.out.println(e.getMessage)
        }
    
    /**
     * Save the vector as text file using the coordinates
     * @type {String => Unit}
     */
    def saveAsTextFile(path: String) = entries.saveAsTextFile(path)
    
    /**
     * Convert the vector to a local vector if possible
     * @type {RDD[(Long, Double)] => Vector}
     */
    def toVector: Vector = Vectors.dense(this.toArray)

    // The following methods are used for test purpose only and so is private to the package sparse
    private[sparse] def toBreeze = DenseVector(this.toArray)

    private[sparse] def dot(that: LongVector): Double = {
        if (this.length < Int.MaxValue){
            this.toBreeze dot that.toBreeze
        }else{
            this.entries.join(that.entries).map{
                case(k, (v1, v2)) => (v1 * v2)
            }.reduce(_+_)
        }
    }

    private[sparse] def /(a: Double): LongVector = {
        require(a != 0, "Can't divide the vector by zero!")
        new LongVector(this.entries.mapValues(v => v / a), this.length)
    }
}
