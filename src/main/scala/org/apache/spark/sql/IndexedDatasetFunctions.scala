package org.apache.spark.sql

import indexeddataframe.logical.{AppendRows, CreateIndex, GetRows}

/**
  * we add 3 new "indexed" methods to the dataset class
  * with these, users can create indexes, append rows and get rows based on key
  * also indexed equi joins are supported if the left side of the join is an indexed relation
  * @param ds
  * @tparam T
  */
class IndexedDatasetFunctions[T](ds: Dataset[T]) extends Serializable {
  def createIndex(colNo: Int): DataFrame = {
    Dataset.ofRows(ds.sparkSession, CreateIndex(colNo, ds.logicalPlan))
  }
  def appendRows(rightDS: Dataset[T]): DataFrame = {
    Dataset.ofRows(ds.sparkSession, AppendRows(ds.logicalPlan, rightDS.logicalPlan))
  }
  def getRows(key: AnyVal): DataFrame = {
    Dataset.ofRows(ds.sparkSession, GetRows(key, ds.logicalPlan))

  }
}