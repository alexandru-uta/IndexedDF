package indexeddataframe


import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, JoinedRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}
import org.apache.spark.sql.Row

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer
import indexeddataframe.RowBatch

import scala.collection.mutable

/**
  * Created by alexuta on 10/07/17.
  * this is a mockup of the internal data structure that stores the indexed data frame
  */
class InternalIndexedDF[K] {

  private val index:TrieMap[K, Int] = new TrieMap[K, Int]
  private var schema:StructType = null
  private var indexCol:Int = 0

  //private val rows:ArrayBuffer[InternalRow] = new ArrayBuffer[InternalRow]()
  private val rowPointers:ArrayBuffer[Int] = new ArrayBuffer[Int]()
  private var nRows:Int = 0

  private val rowBatches = new ArrayBuffer[RowBatch]
  private var nRowBatches = 0
  // variable that keeps track of row length, batch number and batch offset
  private var rowBatchData = new ArrayBuffer[(Int, Int, Int)]

  /**
    * function that creates a row batch
    */
  private def createRowBatch() = {
    rowBatches.append(new RowBatch())
    nRowBatches += 1
  }

  /**
    * function that returns a row batch which can fit the current row
    * @param row
    * @return
    */
  private def getBatchForRow(row: Array[Byte]) = {
    if (rowBatches(nRowBatches - 1).canInsert(row) == false) {
      // if this row cannot fit, create a new batch
      createRowBatch()
    }
    rowBatches(nRowBatches - 1)
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
    this.indexCol = columnNo
    createRowBatch()
    //println(this.schema)
  }

  /**
    * method that appends a row to the local indexed data frame partition
    * @param row
    */
  def appendRow(row: InternalRow) = {
    // get the current row byte array
    val rowData = row.asInstanceOf[UnsafeRow].getBytes()
    // get the current batch
    val crntBatch = getBatchForRow(rowData)
    val offset = crntBatch.appendRow(rowData)
    // keep track of row len, batch no and offset in batch
    rowBatchData.append((rowData.length, nRowBatches - 1, offset))
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
  def appendRows(rows: Iterator[(Long, InternalRow)]) = {
    rows.foreach( row => {
      appendRow(row._2)
    })
  }

  /**
    * iterator returned by the get function on the indexed data frame
    */
  class RowIterator(rowId: Int) extends Iterator[InternalRow] {
    // unsafeRow object that points to the byte array representing its data
    private val currentRow = new UnsafeRow(schema.size)
    private var crntRowId = rowId

    def hasNext(): Boolean = {
      var ret = false
      if (crntRowId != -1) {
        ret = true
      }
      ret
    }
    def next(): InternalRow = {
      val ptrToRowBatch = rowBatchData(crntRowId)
      val rowlen = ptrToRowBatch._1
      val batchNo = ptrToRowBatch._2
      val batchOffset = ptrToRowBatch._3
      val rowBytes = rowBatches(batchNo).getRow(batchOffset, rowlen)
      //ret.pointTo(rowBatches(batchNo).rowData, batchOffset, rowlen)
      currentRow.pointTo(rowBytes, rowlen)
      //val ret = rows(crntRowId).copy()
      this.crntRowId = rowPointers(crntRowId)
      currentRow
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
    //val rows = new ArrayBuffer[InternalRow]
    new Iterator[InternalRow]() {
      val currentRow = new UnsafeRow(schema.size)
      var rowIndex = 0

      override def hasNext: Boolean = {
        rowIndex < nRows
      }

      override def next(): InternalRow = {
        val data = rowBatchData(rowIndex)
        rowIndex += 1

        val rowlen = data._1
        val batchNo = data._2
        val rowOffset = data._3

        currentRow.pointTo(rowBatches(batchNo).getRow(rowOffset, rowlen), rowlen)
        currentRow
      }
    }
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
  def multigetJoined(keys: Iterator[(Long, InternalRow)], output: Seq[Attribute]): Iterator[InternalRow] = {
    val proj = UnsafeProjection.create(output, output)
    val joined = new JoinedRow()
    keys.flatMap { keyAndRow =>
      val key = keyAndRow._1
      val right = keyAndRow._2
      joined.withRight(right)
      get(key.asInstanceOf[K]).map { left =>
        proj(joined.withLeft(left))
      }
    }
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

        resultArray.append(joinedRow)
      }
    }
    val result = resultArray.toIterator.map( joinedRow => proj(joinedRow))
    val t2 = System.nanoTime()
    val totTime = (t2 - t1) / 1000000.0
    println("multiget %f time, looked up %d rows, returned  %d rows, %d unique, tput = %f rows/ms, ctrie tput = %f lookups/ms".format(totTime, size, resultArray.size, uniqueKeys, resultArray.size/totTime, size/totTime))

    result
  }
}
