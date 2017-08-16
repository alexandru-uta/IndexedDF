package indexeddataframe

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, JoinedRow, UnsafeProjection}
import org.apache.spark.sql.execution.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer

/**
  * Created by alexuta on 10/07/17.
  * this is a mockup of the internal data structure that stores the indexed data frame
  */
class InternalIndexedDF[K] {

  private val batchSize = 16384
  private val index:TrieMap[K, Int] = new TrieMap[K, Int]
  private val cBatches:ConcurrentHashMap[Int, ColumnarBatch] = new ConcurrentHashMap[Int, ColumnarBatch]()
  private var schema:StructType = null
  private var indexCol:Int = 0
  private var nBatches:AtomicInteger = new AtomicInteger(0)

  private val rows:ArrayBuffer[InternalRow] = new ArrayBuffer[InternalRow]()
  private val rowPointers:ArrayBuffer[Int] = new ArrayBuffer[Int]()
  private var nRows:Int = 0

  /**
    * creates a columnar batch based on the schema
    * @return the created columnar batch
    */
  private def createColumnarBatch(): ColumnarBatch = {
    // create a columnar batch
    val cBatch = ColumnarBatch.allocate(this.schema)
    var i = 0
    // create the columns based on the schema
    while (i < this.schema.size) {
      val crntVector = ColumnVector.allocate(batchSize, this.schema(i).dataType, MemoryMode.ON_HEAP)
      cBatch.setColumn(i, crntVector)
      i += 1
    }
    cBatches.put(nBatches.get(), cBatch)
    nBatches.getAndIncrement()
    cBatch
  }

  /**
    * function that creates the index of the df on column with index columnNo
    */
  def createIndex(df: DataFrame, columnNo: Int) = {
    // get the schema of the df
    this.schema = df.schema
    // create a new column and add it to the schema
    val prevCol = new StructField("prev", IntegerType, true)
    this.schema = this.schema.add(prevCol)
    this.indexCol = columnNo
    //println(this.schema)
  }

  /**
    * function that sets the schema given a list of data types
    * and also stores the index column
    * @param types
    * @param columnNo
    */
  def createIndex(types: Seq[DataType], columnNo: Int) = {
    this.schema = new StructType()
    for (ty <- types) {
      val field = new StructField("", ty)
      this.schema = this.schema.add(field)
    }
    val prevCol = new StructField("prev", IntegerType, true)
    this.schema = this.schema.add(prevCol)
    this.indexCol = columnNo
    //println(this.schema)
  }

  /**
    * method that appends a row to the local indexed data frame partition
    * @param row
    */
  def appendRow(row: InternalRow) = {
    this.rows.append(row.copy())
    // check if the row already exists in the ctrie
    val key = row.get(this.indexCol, schema.fields(this.indexCol).dataType)
    val value = index.get(key.asInstanceOf[K])
    if (value != None) {
      // key already exists
      // set the pointer of this row to point to previous row
      this.rowPointers.append(value.get)
      this.index.put(key.asInstanceOf[K], this.nRows)
    } else {
      // key does not exist
      this.index.put(key.asInstanceOf[K], this.nRows)
      this.rowPointers.append(-1)
    }
    //println("we just inserted key %s, rowid = %d".format(key.asInstanceOf[String], rowPointers(this.nRows)))
    this.nRows += 1
  }

  /**
    * method that appends a list of InternalRow
    * @param rows
    */
  def appendRows(rows: Seq[InternalRow]) = {
    rows.foreach( row => {
      appendRow(row)
    })
  }

  /**
    * same as before but now the the input is an array of rows
    * @param rows
    */
  def appendRows(rows: Array[Row]) = {
    rows.foreach( row => {
      appendRow(InternalRow.fromSeq(row.toSeq))
    })
  }

  /**
    * iterator returned by the get function on the indexed data frame
    */
  class RowIterator(rowId: Int) extends Iterator[InternalRow] {
    private var crntRowId = rowId
    def hasNext(): Boolean = {
      var ret = false
      if (crntRowId != -1) {
        ret = true
      }
      ret
    }
    def next(): InternalRow = {
      val ret = rows(crntRowId).copy()
      this.crntRowId = rowPointers(crntRowId)
      ret
    }
  }
  /**
    * function that performs lookups in the indexed data frame
    * returns an iterator of rows
   */
  def get(key: K): Iterator[InternalRow] = {
    val firstRowId = index.get(key)
    var ret: Iterator[InternalRow] = null
    if (firstRowId != None) ret = new RowIterator(firstRowId.get)
    else ret = new RowIterator(-1)
    ret
  }

  /**
    * iterator through an array of InternalRow
    */
  class ScanIterator(rows: ArrayBuffer[InternalRow]) extends Iterator[InternalRow] {
    val nElems = rows.size
    var crntElem = 0
    def hasNext(): Boolean = {
      (crntElem < nElems)
    }
    def next(): InternalRow = {
      val ret = rows(crntElem)
      crntElem += 1
      ret
    }
    override def size: Int = {nElems}
    override def length: Int = {nElems}
  }

  /**
    * iterator function imposed by the RDD interface
    */
  def iterator(): Iterator[InternalRow] = {
    /* val resArray = new ArrayBuffer[InternalRow]
    val iter = index.iterator
    while (iter.hasNext) {
      val crntEntry = iter.next()
      val key = crntEntry._1
      val rowid = crntEntry._2

      val rows = get(key)
      while (rows.hasNext) {
        resArray.append(rows.next())
      }
    } */
    new ScanIterator(rows)
  }

  /**
    * returns the number of rows in this partition
    * @return
    */
  def size = { nRows }


  /**
    * a multiget function that returns an Iterator of InternalRow
    * @param keys
    * @return
    */
  def multiget(keys: Array[K]): Iterator[InternalRow] = {
    //println("multiget input size = " + keys.size)
    val resArray = new ArrayBuffer[InternalRow]
    keys.foreach( key => {
      val rowIter = get(key)
      while (rowIter.hasNext) {
        resArray.append(rowIter.next())
      }
    })
    //println("multiget size = " + resArray.size)
    if (resArray.size > 0) new ScanIterator(resArray)
    else new ScanIterator(new ArrayBuffer[InternalRow](0))
  }

  /**
    * a similar multiget, but this one returns joined rows, composed of left + right joined rows
    * we need the projection as a parameter to convert back to unsafe rows
    * @param keys
    * @return
    */
  def multigetJoined(keys: Array[(Long, InternalRow)], output: Seq[Attribute]): Iterator[InternalRow] = {
    val t1 = System.nanoTime()
    val resultArray = new ArrayBuffer[InternalRow]
    val proj = UnsafeProjection.create(output, output.map(_.withNullability(true)))

    var uniqueKeys = 0

    var i = 0
    val size = keys.size
    while (i < size) {
      val pair = keys(i)
      i += 1

      val key = pair._1
      val row = pair._2

      val localRows = get(key.asInstanceOf[K])
      if (localRows.hasNext) uniqueKeys += 1
      while (localRows.hasNext) {
        val localRow = localRows.next()

        val joinedRow = new JoinedRow
        joinedRow.withLeft(localRow)
        joinedRow.withRight(row)

        resultArray.append(proj(joinedRow).copy())
      }
    }
    val result = resultArray.toIterator
    val t2 = System.nanoTime()
    val totTime = (t2 - t1) / 1000000.0
    println("multiget %f time, looked up %d rows, returned  %d rows, %d unique, tput = %f rows/ms, ctrie tput = %f lookups/ms".format(totTime, size, resultArray.size, uniqueKeys, resultArray.size/totTime, size/totTime))

    result
  }

  /**
    * a similar multiget, but this one returns joined rows, composed of left + right joined rows
    * we need the projection as a parameter to convert back to unsafe rows
    * @param keys
    * @return
    */
  def multigetBroadcast(keys: Array[InternalRow], output: Seq[Attribute]): Iterator[InternalRow] = {
    val t1 = System.nanoTime()
    val resultArray = new ArrayBuffer[InternalRow]
    val proj = UnsafeProjection.create(output, output.map(_.withNullability(true)))

    var uniqueKeys = 0

    var i = 0
    val size = keys.size
    while (i < size) {
      val row = keys(i)
      i += 1

      val key = row.get(this.indexCol, schema.fields(this.indexCol).dataType).asInstanceOf[Long]

      val localRows = get(key.asInstanceOf[K])
      if (localRows.hasNext) uniqueKeys += 1
      while (localRows.hasNext) {
        val localRow = localRows.next()

        val joinedRow = new JoinedRow
        joinedRow.withLeft(localRow)
        joinedRow.withRight(row)

        resultArray.append(proj(joinedRow).copy())
      }
    }
    val result = resultArray.toIterator
    val t2 = System.nanoTime()
    val totTime = (t2 - t1) / 1000000.0
    println("multiget %f time, looked up %d rows, returned  %d rows, %d unique, tput = %f rows/ms, ctrie tput = %f lookups/ms".format(totTime, size, resultArray.size, uniqueKeys, resultArray.size/totTime, size/totTime))

    result
  }
}
