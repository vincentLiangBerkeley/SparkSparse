package sparse

import org.scalatest.FunSuite
import org.apache.spark.SparkContext
import breeze.linalg._
import org.apache.log4j.Level
import org.apache.log4j.Logger

class GMSuite() extends FunSuite with LocalSparkContext {
    trait TestEnv{
        val filesReal = List("bfw398a.mtx", "cry2500.mtx", "fidapm05.mtx", "pores_1.mtx", "mcca.mtx")
        val filePath = "/Users/Vincent/Documents/GSI/MATH221/Project/SparkSparse/"  
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)  
    }

    test("Testing on bfw398a.mtx"){
        new TestEnv{
            sc = new SparkContext("local[4]", "test")
            
            val IOobject = new MatrixVectorIO(filePath + "matrices/", sc)
            val matrix = IOobject.readMatrixGraph(filesReal(1), 8)
            val length = matrix.numCols

            val vector = SparseUtility.randomVector(0, 1, length)
            val temp = matrix.multiply(vector, sc)

            val localMatrix = matrix.toBreeze
            val localVector = DenseVector(vector.toArray)

            val start = System.currentTimeMillis
            val sparkResult = matrix multiply(vector, sc)
            val end = System.currentTimeMillis
            System.out.println("The running time is " + (end - start) + "ms.")

            val result = DenseVector(sparkResult.toArray)

            assert(max(result - localMatrix * localVector) < 0.0001)
        }
    }
}