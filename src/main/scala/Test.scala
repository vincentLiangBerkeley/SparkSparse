import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import sparse._
import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.distributed._
import org.apache.log4j.Level
import org.apache.log4j.Logger


// This is just a test of multiplication using a simple implementation of power method
object Test {
    def testOnce(matrix: Multipliable, vector: Vector, sc: SparkContext, numTests: Int) = {
        var v: Vector = vector
        var time: Double = 0.0
        for( i <- 1 to numTests) {
            val start = System.currentTimeMillis
            val result = matrix multiply(v, sc)
            val end = System.currentTimeMillis
            if(i > 1) time += (end - start)
            if(i == 1) System.out.println("Data processing time is " + (end - start) + "ms.")
            //System.out.println("Iteration " + i + " takes " + (end - start) + "ms.")
        }
        System.out.println("Average multiplication time is " + time / (numTests - 1) + "ms.")
    }

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Multiplication Test")
        val sc = new SparkContext(conf)
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)  

        val numTests: Int = if(args.length > 0) args(0).toInt else 20

        val name: String = if (args.length > 1) args(1) else "cry2500.mtx"

        val graphTest: Boolean = if (args.length > 2) args(2) else false

        val filePath = "./matrices/"    
        val IOobject = new MatrixVectorIO(filePath, sc)

        System.out.println("Testing multiplication on " + name + " using COO format")
        val coo = IOobject.readMatrix(name, 8)
        val length = coo.numCols
        // Random vector as starting point
        val vector = SparseUtility.randomVector(0, 1, length) 
        testOnce(coo, vector, sc, numTests)

        System.out.println("Testing multiplication on " + name + " using CSC format")
        val csc = IOobject.readMatrix(name, "CSC", 8)
        testOnce(csc, vector, sc, numTests)

        if (graphTest){
            System.out.println("Testing multiplication on " + name + " using Graph format")
            val gm = IOobject.readMatrixGraph(name, 8)
            testOnce(gm, vector, sc, numTests)
        }  
    }
}