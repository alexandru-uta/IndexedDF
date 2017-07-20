package indexeddataframe


import org.apache.spark.sql.{Dataset, IndexedDatasetFunctions}

object implicits {
  import scala.language.implicitConversions

  implicit def datasetToIndexedDatasetFunctions[T](ds: Dataset[T]): IndexedDatasetFunctions[T] =
    new IndexedDatasetFunctions[T](ds)
}
