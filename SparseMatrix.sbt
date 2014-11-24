name := "SparseMatrix"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-graphx" % "1.1.0",
    "org.apache.spark" %% "spark-core_2.10" % "1.1.0",
    "org.apache.spark" %% "spark-mllib_2.10" % "1.1.0",
    "org.scalatest" % "scalatest_2.10" % "2.0" % "test",
    "org.scalanlp" %% "breeze" % "0.10",
    "org.scalanlp" %% "breeze-natives" % "0.10"
)

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"