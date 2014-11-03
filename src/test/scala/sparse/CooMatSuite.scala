package sparse

import org.scalatest.FunSuite
import org.apache.spark.SparkContext
import breeze.linalg._
import org.apache.log4j.Level
import org.apache.log4j.Logger

class CooMatSuite extends FunSuite with LocalSparkContext{
    trait TestEnv {
        val files = List("fidap005.mtx", "fidapm05.mtx", "pores_1.mtx")
        val filePath = "/Users/Vincent/Documents/GSI/MATH221/Project/" 
        val outputPath = filePath + "output/"
        val vectorPath = filePath + "vectors/" 
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)  
    }

    test("Multiplication of fidap005"){
        new TestEnv{
            sc = new SparkContext("local", "test")
            
            val IOobject = new MatrixVectorIO(filePath + "matrices/", sc)
            val matrix = IOobject.readMatrix(files(0))
            val length = matrix.numCols
            
            val vector = SparseUtility.randomVector(0, 1, length)

            val sparkResult = matrix multiply(vector, sc)

            val localMatrix = matrix.toBreeze
            val localVector = DenseVector(vector.toArray)

            val result = DenseVector(sparkResult.toArray)

            assert(max(result - localMatrix * localVector) < 0.0001)
        }
    }

    test("Multiplication of fidapm05"){
        new TestEnv{
            sc = new SparkContext("local", "test")
            
            val IOobject = new MatrixVectorIO(filePath + "matrices/", sc)
            val matrix = IOobject.readMatrix(files(1))
            val length = matrix.numCols
            
            val vector = SparseUtility.randomVector(0, 1, length)

            val sparkResult = matrix multiply(vector, sc)

            val localMatrix = matrix.toBreeze
            val localVector = DenseVector(vector.toArray)

            val result = DenseVector(sparkResult.toArray)

            assert(max(result - localMatrix * localVector) < 0.0001)
        }
    }

    test("Multiplication of pores_1"){
        new TestEnv{
            sc = new SparkContext("local", "test")
            
            val IOobject = new MatrixVectorIO(filePath + "matrices/", sc)
            val matrix = IOobject.readMatrix(files(2))
            val length = matrix.numCols
            val vector = SparseUtility.randomVector(0, 1, length)

            val sparkResult = matrix multiply(vector, sc)

            val localMatrix = matrix.toBreeze
            val localVector = DenseVector(vector.toArray)

            val result = DenseVector(sparkResult.toArray)

            assert(max(result - localMatrix * localVector) < 0.0001)
        }
    }

}