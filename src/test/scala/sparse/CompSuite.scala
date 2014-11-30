package sparse

import org.scalatest.FunSuite
import org.apache.spark.SparkContext
import breeze.linalg._
import org.apache.log4j.Level
import org.apache.log4j.Logger

class CompSuite() extends FunSuite with LocalSparkContext {
    trait TestEnv{
        val filesReal = List("bfw398a.mtx", "cry2500.mtx", "fidapm05.mtx", "pores_1.mtx", "mcca.mtx")
        val filePath = "/Users/Vincent/Documents/GSI/MATH221/Project/SparkSparse/"  
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)  
    }

    test("On multiplication of bfw398a.mtx, 398 * 398 real matrix"){
        new TestEnv{
            sc = new SparkContext("local[4]", "test")
            
            val IOobject = new MatrixVectorIO(filePath + "matrices/", sc)
            val cooMat = IOobject.readMatrix(filesReal(0))
            val csrMat = IOobject.readMatrixCSC(filesReal(0))
            val graphMat = IOobject.readMatrixGraph(filesReal(0))

            val length = cooMat.numCols

            val vector = SparseUtility.randomVector(0, 1, length)

            for( i <- 1 to 10) {
                
            }
        }
    }
}