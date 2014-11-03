/*
This object serves as an utility IO object that reads in a sparse matrix in Matrix Market format
as a dense breeze matrix locally, just for test purpose.
 */
package sparse

import scala.io.Source
import breeze.linalg._

// The path variable should be the path where the matrices are stored
class BreezeIO(path: String) {
    private val filePath = path

    // This function basically just decodes the Matrix Market format and return a dense matrix
    def readMatrix(name: String): DenseMatrix[Double] = {
        val lines = Source.fromFile(filePath + name).getLines().toArray
        val sizeInfo = lines(1).split(' ').filter(s => s.length > 0);
        val nRows = sizeInfo(0).toInt
        val nCols = sizeInfo(1).toInt
        val nEntries = sizeInfo(2).toInt
        //System.out.println("The number of lines of the file is " + lines.length)
        //System.out.println("The number of entries in the file is " + nEntries)

        // I believe this generates a zero matrix
        val matrix = DenseMatrix.zeros[Double](nRows, nCols)

        // Update the nonzero entries in the matrix
        for( i <- 2 until lines.length) {
            val entry = lines(i).split(' ').filter(s => s.length > 0)
            //entry.foreach(println)
            val rowInd = entry(0).toInt
            val colInd = entry(1).toInt
            val value = entry(2).toDouble

            //matrix(rowInd - 1, colInd - 1):= value -> This line does not work
            matrix.update(rowInd - 1, colInd - 1, value)
        }

        matrix
    }

    // This function reads in a vector from text file, in case specific vectors are used for 
    // tests rather than random vectors
    def readVector(name: String) = {
        val lines = Source.fromFile(filePath + name).getLines().toArray
        lines.map(x => x.toDouble)

        DenseVector(lines)
    }
}