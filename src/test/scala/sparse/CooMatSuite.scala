package sparse

import org.scalatest.FunSuite
import org.apache.spark.SparkContext
import breeze.linalg._
import org.apache.log4j.Level
import org.apache.log4j.Logger

class CooMatSuite extends FunSuite with LocalSparkContext{
    trait TestEnv {
        val filesReal = List("fidap005.mtx", "fidapm05.mtx", "pores_1.mtx", "mcca.mtx", "bcspwr04.mtx", "ash219.mtx", "orani678.mtx")
        val filePath = "/Users/Vincent/Documents/GSI/MATH221/Project/SparkSparse/"  
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)  
    }

    test("Multiplication of fidap005"){
        new TestEnv{
            sc = new SparkContext("local", "test")
            
            val IOobject = new MatrixVectorIO(filePath + "matrices/", sc)
            val matrix = IOobject.readMatrix(filesReal(0))
            val length = matrix.numCols
            
            val vector = SparseUtility.randomVector(0, 1, length)
            val temp = matrix multiply(vector, sc)

            val start = System.currentTimeMillis
            val sparkResult = matrix multiply(vector, sc)
            val end = System.currentTimeMillis

            val localMatrix = matrix.toBreezeMat
            val localVector = DenseVector(vector.toArray)

            val result = DenseVector(sparkResult.toArray)

            
            val startLoal = System.currentTimeMillis
            assert(max(result - localMatrix * localVector) < 0.0001)
            val endLocal = System.currentTimeMillis

            System.out.println("The running time is " + (end - start) + "ms.")
            System.out.println("The local running time is " + (endLocal - startLoal) + "ms")
        }
    }

    test("Multiplication of orani678.mtx"){
        new TestEnv{
            sc = new SparkContext("local", "test")
            
            val IOobject = new MatrixVectorIO(filePath + "matrices/", sc)
            val matrix = IOobject.readMatrix(filesReal(6))
            val length = matrix.numRows
            
            val vector = SparseUtility.randomVector(0, 1, length)
            val temp = matrix multiply(vector, sc)

            val start = System.currentTimeMillis
            val sparkResult = matrix multiply(vector, sc)
            val end = System.currentTimeMillis

            val localMatrix = matrix.toBreezeMat
            val localVector = DenseVector(vector.toArray)

            val result = DenseVector(sparkResult.toArray)

            
            val startLoal = System.currentTimeMillis
            assert(max(result - localMatrix * localVector) < 0.0001)
            val endLocal = System.currentTimeMillis

            System.out.println("The running time is " + (end - start) + "ms.")
            System.out.println("The local running time is " + (endLocal - startLoal) + "ms")
        }
    }

    test("Multiplication of mcca"){
        new TestEnv{
            sc = new SparkContext("local", "test")
            
            val IOobject = new MatrixVectorIO(filePath + "matrices/", sc)
            val matrix = IOobject.readMatrix(filesReal(3))
            val length = matrix.numCols
            
            val vector = SparseUtility.randomVector(0, 1, length)
            val temp = matrix multiply(vector, sc)

            val start = System.currentTimeMillis
            val sparkResult = matrix multiply(vector, sc)
            val end = System.currentTimeMillis

            val localMatrix = matrix.toBreezeMat
            val localVector = DenseVector(vector.toArray)

            val result = DenseVector(sparkResult.toArray)

            
            val startLoal = System.currentTimeMillis
            assert(max(result - localMatrix * localVector) < 0.0001)
            val endLocal = System.currentTimeMillis

            System.out.println("The running time is " + (end - start) + "ms.")
            System.out.println("The local running time is " + (endLocal - startLoal) + "ms")
        }
    }

    test("Multiplication of bcspwr04.mtx, pattern symmetric matrix"){
        new TestEnv{
            sc = new SparkContext("local", "test")
            
            val IOobject = new MatrixVectorIO(filePath + "matrices/", sc)
            val matrix = IOobject.readMatrix(filesReal(4))
            val length = matrix.numCols
            
            val vector = SparseUtility.randomVector(0, 1, length)
            val temp = matrix multiply(vector, sc)

            val start = System.currentTimeMillis
            val sparkResult = matrix multiply(vector, sc)
            val end = System.currentTimeMillis

            val localMatrix = matrix.toBreezeMat
            val localVector = DenseVector(vector.toArray)

            val result = DenseVector(sparkResult.toArray)

            val startLoal = System.currentTimeMillis
            assert(max(result - localMatrix * localVector) < 0.0001)
            val endLocal = System.currentTimeMillis

            System.out.println("The running time is " + (end - start) + "ms.")
            System.out.println("The local running time is " + (endLocal - startLoal) + "ms")
        }
    }

    test("Multiplication of ash219.mtx, pattern matrix"){
        new TestEnv{
            sc = new SparkContext("local", "test")
            
            val IOobject = new MatrixVectorIO(filePath + "matrices/", sc)
            val matrix = IOobject.readMatrix(filesReal(5))
            val length = matrix.numCols
            
            val vector = SparseUtility.randomVector(0, 1, length)
            val temp = matrix multiply(vector, sc)
            
            val start = System.currentTimeMillis
            val sparkResult = matrix multiply(vector, sc)
            val end = System.currentTimeMillis

            val localMatrix = matrix.toBreezeMat
            val localVector = DenseVector(vector.toArray)

            val result = DenseVector(sparkResult.toArray)

            val startLoal = System.currentTimeMillis
            assert(max(result - localMatrix * localVector) < 0.0001)
            val endLocal = System.currentTimeMillis

            System.out.println("The running time is " + (end - start) + "ms.")
            System.out.println("The local running time is " + (endLocal - startLoal) + "ms")
        }
    }

}