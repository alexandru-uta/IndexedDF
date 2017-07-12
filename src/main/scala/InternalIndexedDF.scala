package indexeddataframe;

import scala.collection.concurrent.TrieMap
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Created by alexuta on 10/07/17.
  * this is a mockup of the internal data structure that stores the indexed data frame
  */
class InternalIndexedDF {

  private val batchSize = 16384
  private val index:TrieMap[String, Int] = new TrieMap[String, Int]
  private val cBatches:ConcurrentHashMap[Int, ColumnarBatch] = new ConcurrentHashMap[Int, ColumnarBatch]()
  private var schema:StructType = null
  private var indexCol:Int = 0
  private var nBatches:AtomicInteger = new AtomicInteger(0)
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
  }

  def appendRows(rows: Array[Row]) = {
    var counter = 0
    var crntBatch:ColumnarBatch = null

    rows.foreach(row => {
      //println(row.toString())
      if (counter % batchSize == 0) {
        // we reached max capacity
        // get a new empty batch
        crntBatch = createColumnarBatch()
      }
      // put the row in the columnar batch
      var i = 0
      while (i < row.size) {
        // get the column
        val crntColumn = crntBatch.column(i)
        val crntElem = row(i)
        // check which type the element is then insert it properly
        val dType = this.schema(i).dataType
        if (dType == IntegerType) { crntColumn.appendInt(crntElem.asInstanceOf[Int]) }
        else {//if (dType == StringType) {
          val crntStr = crntElem.asInstanceOf[String]
          val bytes = crntStr.getBytes()
          crntColumn.appendByteArray(bytes, 0, bytes.length)
        }
        i += 1

        //println("row %d element %d is: %s".format(counter, i, crntElem.asInstanceOf[String]))
      }
      // make the prev column null or -1
      crntBatch.column(this.schema.size - 1).appendInt(-1)

      // get the value of the indexed column
      val crntIndex = row(indexCol)
      // convert to string and then search for it
      val crntIndexVal = crntIndex.asInstanceOf[String]
      // get the value in the cTree
      val value = index.get(crntIndexVal)
      // if this key already exists
      if (value != None) {
        //println("equal key = %s, counter = %d".format(crntIndexVal, counter))
        // put the current row id
        index.put(crntIndexVal, counter)
        // update the prev field of the current row to point to the prev rowid
        val rowid = value.get
        cBatches.get(counter / batchSize).column(this.schema.size - 1).putInt(counter % batchSize, rowid)
      } else {
        // this key does not already exist
        index.put(crntIndexVal, counter)
      }

      counter += 1
    })

  }

  /**
    * iterator returned by the get function on the indexed data frame
    */
  class RowIterator(rowId: Int, cBatches:ConcurrentHashMap[Int, ColumnarBatch]) extends Iterator[InternalRow] {
    private var crntRowId = rowId
    def hasNext(): Boolean = {
      var ret = false
      if (crntRowId != -1) {
        ret = true
      }
      ret
    }
    def next(): InternalRow = {
      val ret = cBatches.get(crntRowId / batchSize).getRow(crntRowId % batchSize)
      this.crntRowId = ret.getInt(ret.numFields() - 1)
      ret
    }
  }
  /**
    * function that performs lookups in the indexed data frame
    * returns an iterator of rows
   */
  def get(key: String): Iterator[InternalRow] = {
    val firstRowId = index.get(key)
    if (firstRowId != None) {
        val ret: Iterator[InternalRow] = new RowIterator(firstRowId.get, cBatches)
        ret
    } else {
      null
    }
  }
}
