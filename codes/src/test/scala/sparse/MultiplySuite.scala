package sparse

import org.scalatest.FunSuite
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, CoordinateMatrix}

class MultiplySuite extends FunSuite with LocalSparkContext{
    trait TestEnv {
        val files = List("fidap005.mtx", "fidapm05.mtx", "pores_1.mtx")
        val filePath = "/Users/Vincent/Documents/GSI/MATH221/Project/" 
        val outputPath = filePath + "output/"
        val vectorPath = filePath + "vectors/" 
        //Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        //Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)  
    }

    // test("Multiplying with fidap005.mtx") {
    //    new TestEnv{
    //     sc = new SparkContext("local", "test")
    //     System.out.println("Reading in matrix...")
    //     val IOobject = new MatrixVectorIO(filePath + "matrices/", sc)
    //     val matrix = IOobject.readMatrix(files(0))
    //     val length = matrix.numCols
    //     System.out.println(length)
    //     val vector = SparseUtility.randomVector(0, 1, length)
        

        // val breezeIO = new BreezeIO(filePath + "matrices/")
        // val localMatrix = breezeIO.readMatrix(files(0))
        // val localVector = DenseVector(vector.toArray) 

        // val sparkResult = SparseUtility.multiply(matrix, vector, sc)
        // val transFormedSparkResult = DenseVector(sparkResult.toArray)
        // val localResult = localMatrix * localVector

        // assert(max(transFormedSparkResult - localResult) < 0.01)
      //}
    //}
    //
    test("Just a new test") {
        new TestEnv{
            sc = new SparkContext("local", "test")
            val testArray = Array[MatrixEntry](new MatrixEntry(0, 0, 1.0), new MatrixEntry(1, 1, 2.0))
            val entries = sc.parallelize(testArray)
            System.out.println(entries.count)
            val matrix = new CoordinateMatrix(entries)

            System.out.println(matrix.numCols)
        }
    }

}