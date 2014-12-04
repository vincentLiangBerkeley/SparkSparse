package sparse

import org.scalatest.FunSuite
import org.apache.spark.SparkContext
import breeze.linalg._
import org.apache.log4j.Level
import org.apache.log4j.Logger

class CSCSuite() extends FunSuite with LocalSparkContext {
    trait TestEnv{
        val filesReal = List("bfw398a.mtx", "cry2500.mtx", "bcspwr04.mtx", "fidapm05.mtx", "pores_1.mtx", "mcca.mtx")
        val filePath = "/Users/Vincent/Documents/GSI/MATH221/Project/SparkSparse/"  
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)  
    }

    test("Testing on bfw398a.mtx"){
        new TestEnv{
            sc = new SparkContext("local[4]", "test")
            
            val IOobject = new MatrixVectorIO(filePath + "matrices/", sc)
            val matrix = IOobject.readMatrix(filesReal(0), "CSC", 8)
            val length = matrix.numCols

            val vector = SparseUtility.randomVector(0, 1, length)
            
            val localMatrix = matrix.toBreezeMat
            val localVector = DenseVector(vector.toArray)

            for( i <- 1 to 4) {
                val start = System.currentTimeMillis
                val sparkResult = matrix multiply(vector, sc)
                val end = System.currentTimeMillis

                val result = sparkResult.toBreeze
                assert(max(result - localMatrix * localVector) < 0.0001)
                System.out.println("The running time is " + (end - start) + "ms.")
            }

        }
    }

    test("Testing on cry2500.mtx"){
        new TestEnv{
            sc = new SparkContext("local[4]", "test")
            
            val IOobject = new MatrixVectorIO(filePath + "matrices/", sc)
            val matrix = IOobject.readMatrix(filesReal(1), "CSC", 12)
            val length = matrix.numCols
            
            val vector = SparseUtility.randomVector(0, 1, length)
            val temp = (matrix multiply(vector, sc)).toVector

            val start = System.currentTimeMillis
            val r = matrix multiply(vector, sc)
            val end = System.currentTimeMillis

            
            val localMatrix = matrix.toBreezeMat
            val localVector = DenseVector(vector.toArray)

            val result = r.toBreeze

            val startLoal = System.currentTimeMillis
            assert(max(result - localMatrix * localVector) < 0.0001)
            val endLocal = System.currentTimeMillis

            System.out.println("The running time is " + (end - start) + "ms.")
            System.out.println("The local running time is " + (endLocal - startLoal) + "ms")
        }
    }

    test("Testing on cry2500.mtx with transpose"){
        new TestEnv{
            sc = new SparkContext("local[4]", "test")
            
            val IOobject = new MatrixVectorIO(filePath + "matrices/", sc)
            val matrix = IOobject.readMatrix(filesReal(1), "CSC", 12)
            val length = matrix.numRows
            
            val vector = SparseUtility.randomVector(0, 1, length)
            val temp = matrix multiply(vector, sc, true)

            val start = System.currentTimeMillis
            val sparkResult = matrix multiply(vector, sc, true)
            val end = System.currentTimeMillis

            
            val localMatrix = matrix.toBreezeMat
            val localVector = DenseVector(vector.toArray)

            val result = sparkResult.toBreeze

            val startLoal = System.currentTimeMillis
            assert(max(result - localMatrix.t * localVector) < 0.0001)
            val endLocal = System.currentTimeMillis

            System.out.println("The running time is " + (end - start) + "ms.")
            System.out.println("The local running time is " + (endLocal - startLoal) + "ms")
        }
    }

    test("Testing bcspwr04.mtx, pattern symmetric matrix"){
        new TestEnv{
            sc = new SparkContext("local[4]", "test")
            
            val IOobject = new MatrixVectorIO(filePath + "matrices/", sc)
            val matrix = IOobject.readMatrix(filesReal(2),"CSC", 8)
            val length = matrix.numCols
            
            val vector = SparseUtility.randomVector(0, 1, length)
            val temp = matrix multiply(vector, sc)

            val start = System.currentTimeMillis
            val sparkResult = matrix multiply(vector, sc)
            val end = System.currentTimeMillis

            val localMatrix = matrix.toBreezeMat
            val localVector = DenseVector(vector.toArray)

            val result = sparkResult.toBreeze

            val startLoal = System.currentTimeMillis
            assert(max(result - localMatrix * localVector) < 0.0001)
            val endLocal = System.currentTimeMillis

            System.out.println("The running time is " + (end - start) + "ms.")
            System.out.println("The local running time is " + (endLocal - startLoal) + "ms")
        }
    }
}