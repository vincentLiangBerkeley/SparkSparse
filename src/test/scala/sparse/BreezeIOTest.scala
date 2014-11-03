package sparse

import org.scalatest.FunSuite
import scala.io.Source
import breeze.linalg._

class BreezeIOTest extends FunSuite {
    trait TestEnv {
        val files = List("fidap005.mtx", "fidapm05.mtx", "pores_1.mtx")
        val path = "/Users/Vincent/Documents/GSI/MATH221/Project/matrices/"
    }

    test("Testing BreezeIO on fidap005.mtx"){
        new TestEnv{
            val IOobject = new BreezeIO(path)
            val matrix = IOobject.readMatrix(files(0))

            assert(matrix.cols == 27)
            assert(matrix.rows == 27)
            val lines = Source.fromFile(path + files(0)).getLines().toArray

            // Comparing values at each entry
            for( i <- 2 until lines.length) {
                val entry = lines(i).split(' ').filter(s => s.length > 0)
                val rowInd = entry(0).toInt
                val colInd = entry(1).toInt
                val value = entry(2).toDouble

                assert(matrix(rowInd - 1, colInd - 1) == value)
            }
        }
    }

    test("Testing BreezeIO on fidapm05.mtx"){
        new TestEnv{
            val IOobject = new BreezeIO(path)
            val matrix = IOobject.readMatrix(files(1))

            assert(matrix.cols == 42)
            assert(matrix.rows == 42)
            val lines = Source.fromFile(path + files(1)).getLines().toArray

            for( i <- 2 until lines.length) {
                val entry = lines(i).split(' ').filter(s => s.length > 0)
                val rowInd = entry(0).toInt
                val colInd = entry(1).toInt
                val value = entry(2).toDouble

                assert(matrix(rowInd - 1, colInd - 1) == value)
            }
        }
    }

    test("Testing BreezeIO on pores_1.mtx"){
        new TestEnv{
            val IOobject = new BreezeIO(path)
            val matrix = IOobject.readMatrix(files(2))

            assert(matrix.cols == 30)
            assert(matrix.rows == 30)
            val lines = Source.fromFile(path + files(2)).getLines().toArray

            for( i <- 2 until lines.length) {
                val entry = lines(i).split(' ').filter(s => s.length > 0)
                val rowInd = entry(0).toInt
                val colInd = entry(1).toInt
                val value = entry(2).toDouble

                assert(matrix(rowInd - 1, colInd - 1) == value)
            }
        }
    }
}