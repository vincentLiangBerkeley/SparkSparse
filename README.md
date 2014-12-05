SparkSparse
===========

A temporary sparse utility repository for research purpose.

Just finished the classes, and below is the testing/running method.

Main test class(Object): Test

There are three possible parameters to specify: 
1. numTests:Int -> The number of multiplications used to test each format.
2. matrixName: String -> The name of the matrix(i.e "cry2500.mtx") that is in "./matrices/", default is "cry2500.mtx".
3. graphTest: Boolean -> Flag for whether the graphMatrix format will be tested

Sample running shell script:
$SPARK_HOME/bin/spark-submit --class "Test" --master local[4] target/scala-2.10/sparsematrix_2.10-1.0.jar 40 "af23560.mtx"

The above script will run 20 multiplications on "af23560.mtx" without testing the graph format.
$SPARK_HOME should be your directory with spark installed.
