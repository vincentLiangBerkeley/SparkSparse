package sparse

import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, CoordinateMatrix}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.io.Source
import java.io._

class MatrixVectorIO(path: String, context: SparkContext) {
    private val filePath = path
    private val sc = context
    // Currently can only read in real, patter, integer matrices in general form.
    // @TODO: Add symmetric form to it
    def readMatrix(name: String): CoordinateMatrix = {
        val input = sc.textFile(filePath + name).map(x => x.split(' ').filter(y => y.length > 0))
        val sizeInfo = input.take(2)(1)
        val size = (sizeInfo(0).toLong, sizeInfo(1).toLong)

        // This line of codes drops the first two lines of the file which is already captured 
        val filteredInput = input.mapPartitionsWithIndex{case (index, iter) => if(index == 0) iter.drop(2) else iter}
        
        val entries = filteredInput.map(x => new MatrixEntry(x(0).toLong - 1, x(1).toLong - 1, x(2).toDouble))
        new CoordinateMatrix(entries, size._1, size._2)
    }

    // This routine saves result to a file, for future comparison use
    def saveResult(result: Vector, path: String) = {
        val vector = result.toArray
        val writer = new PrintWriter(new File(path))
        for( i <- 0 until result.size) {
            writer.write(vector(i).toString)
            writer.write("\n")
        }

        writer.close()
    }

    def readVector(name: String): Vector = {
        val entries = Source.fromFile(filePath + name).getLines.map(x => x.toDouble).toArray
        Vectors.dense(entries)
    }
}