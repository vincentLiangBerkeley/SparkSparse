package sparse
    
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector


trait Multipliable {
    def multiply(vector: Vector, sc: SparkContext, trans: Boolean = false): Vector
}