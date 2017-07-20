package indexeddataframe

import org.apache.spark.{OneToOneDependency, Partition, SparkEnv, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DataType

import scala.reflect.ClassTag

/**
  * Created by alexuta on 18/07/17.
  */
object Utils {
  def doIndexing(rows: Seq[InternalRow], types: Seq[DataType]): InternalIndexedDF[String, InternalRow] = {

    val idf = new InternalIndexedDF[String, InternalRow]
    idf.createIndex(types, 0)
    idf.appendInternalRows(rows)

    idf
  }
}

class IRDD[K: ClassTag](private val partitionsRDD: RDD[InternalIndexedDF[K, InternalRow]])
  extends RDD[(K, InternalRow)](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  override val partitioner = partitionsRDD.partitioner

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  override def compute(part: Partition, context: TaskContext): Iterator[(K, InternalRow)] = {
    firstParent[InternalIndexedDF[K, InternalRow]].iterator(part, context).next.iterator
  }

  def get(key: K) = {

  }
}
