name := "IndexedDF"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.1"
libraryDependencies += "org.apache.spark" % "spark-catalyst_2.11" % "2.1.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"


fork in Test := true