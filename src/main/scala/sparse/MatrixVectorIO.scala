package sparse

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark
import scala.io.Source
import java.io._

/**
 * Case class representing the information of a matrix
 * @param entryField: The number field of entries, could be real, pattern
 * @param size: The numRows and numCols of the matrix
 * @param sym: A boolean variable, "true" for matrix being symmetric
 * @param entires: The RDD of entries, in an Array of Strings format
 */
case class MatrixInfo(entryField: String, size: (Long, Long), sym: Boolean, entries: RDD[Array[String]])

class MatrixVectorIO(val filePath: String, val sc: SparkContext) {
    def readMatrix(name: String, partNum: Int = 1): CoordinateMatrix = {
        val info = parseMatrix(name)
        val entryField = info.entryField
        val sym = info.sym
        val data = info.entries
        val size = info.size

        entryField match {
            case "real" | "integer" => 
                val entries = data.map(x => new MatrixEntry(x(0).toLong - 1, x(1).toLong - 1, x(2).toDouble))
                new CoordinateMatrix(entries, size._1, size._2, sym, partNum)
            case "pattern" => 
                val entries = data.map(x => new MatrixEntry(x(0).toLong - 1, x(1).toLong - 1, 1.0))
                new CoordinateMatrix(entries, size._1, size._2, sym, partNum)
        }
    }

    def readMatrixGraph(name: String): GraphMatrix = {
        val info = parseMatrix(name)
        val entryField = info.entryField
        val sym = info.sym
        val data = info.entries
        val size = info.size

        entryField match {
            case "real" | "integer" => 
                val entries = data.map(x => (x(0).toLong - 1, x(1).toLong - 1, x(2).toDouble))
                new GraphMatrix(entries, size._1, size._2)
            case "pattern" => 
                val entries = data.map(x => (x(0).toLong - 1, x(1).toLong - 1, 1.0))
                new GraphMatrix(entries, size._1, size._2)
        }
    }

    def readMatrixCSC(name: String, partNum: Int = 4): CSRMatrix = {
        val info = parseMatrix(name)
        val entryField = info.entryField
        val sym = info.sym
        val data = info.entries
        val size = info.size

        entryField match {
            case "real" | "integer" => 
                val entries = data.map(x => (x(0).toLong - 1, x(1).toLong - 1, x(2).toDouble))
                new CSRMatrix(entries, size._1, size._2, sym, partNum)
            case "pattern" => 
                val entries = data.map(x => (x(0).toLong - 1, x(1).toLong - 1, 1.0))
                new CSRMatrix(entries, size._1, size._2, sym, partNum)
        }
    }

    private def parseMatrix(name: String): MatrixInfo = {
        val input = sc.textFile(filePath + name).map(x => x.split(' ').filter(y => y.length > 0))
        val meta = input.take(2)
        val matInfo = meta(0) // This has the matrix info like "sym", "real", "int", "pattern"
        val sizeInfo = meta(1) // This has the size of the matrix
        val size = (sizeInfo(0).toLong, sizeInfo(1).toLong)

        // The following codes depend on matrix market's matrix format
        val entryField = matInfo(3)
        val matFormat = matInfo(4)

        val sym: Boolean = if(matFormat == "general") false else true
        val filteredInput = input.mapPartitionsWithIndex{case (index, iter) => if(index == 0) iter.drop(2) else iter}

        new MatrixInfo(entryField, size, sym, filteredInput)
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