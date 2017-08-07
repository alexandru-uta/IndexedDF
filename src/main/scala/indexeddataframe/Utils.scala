package indexeddataframe

import indexeddataframe.InternalIndexedDF
import org.apache.spark.{OneToOneDependency, Partition, SparkEnv, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, LongType}
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

/**
  * Created by alexuta on 18/07/17.
  */
object Utils {

  /**
    * function that is executed when the index is created on a DataFrame
    * this function is called for each partition of the original DataFrame and it creates an [[InternalIndexedDF]]
    * that contains the rows of the partitions and a CTrie for storing an index
    * @param colNo
    * @param rows
    * @param types
    * @return
    */
  def doIndexing(colNo: Int, rows: Seq[InternalRow], types: Seq[DataType]): InternalIndexedDF[Long] = {
    val idf = new InternalIndexedDF[Long]
    idf.createIndex(types, colNo)
    idf.appendRows(rows)
    /*
    val iter = idf.get(2199023262994L)
    var nRows = 0
    while (iter.hasNext) {
      nRows += 1
      iter.next()
    }
    println("this item is repeated %d times on this partition".format(nRows))
    */
    idf
  }

  def ensureCached[T](rdd: IRDD, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): IRDD = {
    //println(rdd.getStorageLevel + " --------- " + storageLevel)
    if (rdd.getStorageLevel == StorageLevel.NONE) {
      rdd.persist(storageLevel)
    } else {
      rdd
    }
  }
}

/**
  * a custom RDD class that is composed of a number of partitions containing the rows of the
  * indexed dataframe; each of these partitions is represented as an [[InternalIndexedDF]]
  * @param colNo
  * @param partitionsRDD
  */
class IRDD(private val colNo: Int, private var partitionsRDD: RDD[InternalIndexedDF[Long]])
  extends RDD[InternalRow](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  override val partitioner = partitionsRDD.partitioner

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  override def compute(part: Partition, context: TaskContext): Iterator[InternalRow] = {
    firstParent[InternalIndexedDF[Long]].iterator(part, context).next.iterator
  }

  override def persist(newLevel: StorageLevel): this.type = {
    partitionsRDD = partitionsRDD.persist(newLevel)
    this
  }

  /**
    * RDD function that returns an RDD of the rows containing the search key
    * @param key
    * @return
    */
  def get(key: Long): RDD[InternalRow] = {
    val res = partitionsRDD.mapPartitions[InternalRow](
      part => part.next().get(key)
    )
   res
  }

  def multiget(keys: Seq[Long]): RDD[InternalRow] = {
    val res = partitionsRDD.mapPartitions[InternalRow](
      part => part.next().multiget(keys)
    )
    res
  }

  /**
    * helper function for appending rows in the InternalIndexedDF object
    * @param iter1 -> this is an iterator containing the original data
    * @param iter2 -> this is an iterator containing the rows to be inserted
    * @return -> returns an iterator containing the updated rows
    */
  def appendZipFunc(iter1: Iterator[InternalIndexedDF[Long]], iter2: Iterator[(Long, InternalRow)] ): Iterator[InternalIndexedDF[Long]] = {
    val idf = iter1.next()
    while (iter2.hasNext) {
      val value = iter2.next()._2
      idf.appendRow(value)
      //println("I appended a row!!!")
    }
    Iterator(idf)
  }

  /**
    * RDD function that appends a list of rows to the current data
    * the list of rows is partitioned here such that each row goes to the partition that is supposed to contain that key
    * @param rows
    * @return
    */
  def appendRows(rows: Seq[InternalRow]): IRDD = {
    val map = collection.mutable.Map[Long, InternalRow]()
    rows.foreach( row => {
      // !!! fix later to get the type properly, now we just assume the indexed column is of type long !!!
      val key = row.get(colNo, LongType).asInstanceOf[Long]
      map.put(key, row)
    })
    // make an rdd with the updates
    val updatesRDD = context.parallelize(map.toSeq)
    // partition it similarly to our current partitions
    val partitionedUpdates = updatesRDD.repartition(partitionsRDD.getNumPartitions)
    // apply the updates
    val result = partitionsRDD.zipPartitions(partitionedUpdates)(appendZipFunc)
    // return a new RDD with the appended rows
    new IRDD(colNo, result)
  }
}
