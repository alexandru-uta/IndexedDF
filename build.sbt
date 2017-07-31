name := "IndexedDF"

version := "1.0"

scalaVersion := "2.11.10"

javaOptions += "Xmx4G"
javaOptions += "Xms1G"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.1"
libraryDependencies += "org.apache.spark" % "spark-catalyst_2.11" % "2.1.1"