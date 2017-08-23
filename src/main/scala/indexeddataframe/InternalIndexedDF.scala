package indexeddataframe

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, JoinedRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types._

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer
import indexeddataframe.RowBatch
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowJoiner
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

/**
  * this the internal data structure that stores a partition of an Indexed Data Frame
  */
class InternalIndexedDF {

  // the index data structure
  private val index:TrieMap[Long, Int] = new TrieMap[Long, Int]

  // the schema of the dataframe
  private var schema:StructType = null
  private var output:Seq[Attribute] = null

  // projection that transforms the input row
  // in a row that contains only the indexed column;
  // its result will be subsequently used as a key for the TrieMap
  private var rowToColProj:UnsafeProjection = null

  // the column on which the index is computed
  private var indexCol:Int = 0

  // the row batches in which we keep the row data
  private val rowBatches = new ArrayBuffer[RowBatch]
  private var nRowBatches = 0

  // pointer to #previous row with the same index key
  // we use this to be able to "get" all the rows that contain the index key
  // by crawling through this array
  private val rowPointers:ArrayBuffer[Int] = new ArrayBuffer[Int]()

  // member that keeps track of row length, batch number and batch offset
  private var rowBatchData = new ArrayBuffer[(Int, Int, Int)]

  // the number of inserted rows in this partition
  private var nRows:Int = 0

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
  def createIndex(types: Seq[DataType], output: Seq[Attribute], columnNo: Int) = {
    this.schema = new StructType()
    for (ty <- types) {
      val field = new StructField("", ty)
      this.schema = this.schema.add(field)
    }
    this.indexCol = columnNo
    this.output = output

    // create the projection that obtains an UnsafeRow containing the indexed key for each row
    rowToColProj = UnsafeProjection.create(Seq(output(columnNo)), output)

    createRowBatch()
    //println(this.schema)
  }

  /**
    * method that appends a row to the local indexed data frame partition
    * @param row
    */
  def appendRow(row: InternalRow) = {
    // check the type of the key and transform to long
    val key = schema(indexCol).dataType match  {
      case LongType => row.asInstanceOf[UnsafeRow].getLong(indexCol)
      case IntegerType => row.asInstanceOf[UnsafeRow].getInt(indexCol).toLong
      case StringType => row.asInstanceOf[UnsafeRow].getUTF8String(indexCol).toLong
      case DoubleType => row.asInstanceOf[UnsafeRow].getDouble(indexCol).toLong
      // fall back to long as default
      case _ => row.asInstanceOf[UnsafeRow].getLong(indexCol)
    }
    // get the current row byte array
    val rowData = row.asInstanceOf[UnsafeRow].getBytes()
    // get the current batch
    val crntBatch = getBatchForRow(rowData)
    val offset = crntBatch.appendRow(rowData)
    // keep track of row len, batch no and offset in batch
    rowBatchData.append((rowData.length, nRowBatches - 1, offset))
    // check if the row already exists in the ctrie
    val value = index.get(key)
    if (value != None) {
      // key already exists
      // set the pointer of this row to point to previous row
      this.rowPointers.append(value.get)
      this.index.put(key, this.nRows)
    } else {
      // key does not exist
      this.index.put(key, this.nRows)
      this.rowPointers.append(-1)
    }
    //println("we just inserted key %s, rowid = %d".format(key.asInstanceOf[String], rowPointers(this.nRows)))
    this.nRows += 1
  }

  /**
    * method that appends a list of InternalRow
    * @param rows
    */
  def appendRows(rows: Iterator[InternalRow]) = {
    rows.foreach( row => {
      appendRow(row)
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

      currentRow.pointTo(rowBatches(batchNo).rowData, batchOffset + Platform.BYTE_ARRAY_OFFSET, rowlen)

      this.crntRowId = rowPointers(crntRowId)
      currentRow
    }
  }

  /**
    * function that performs lookups in the indexed data frame
    * returns an iterator of rows
   */
   def get(key: AnyVal): Iterator[InternalRow] = {
     // check the type of the key and transform to long
     val internalKey = schema(indexCol).dataType match  {
       case LongType => key.asInstanceOf[Long]
       case IntegerType => key.asInstanceOf[Int].toLong
       case StringType => key.asInstanceOf[UTF8String].toLong
       case DoubleType => key.asInstanceOf[Double].toLong
       // fall back to long as default
       case _ => key.asInstanceOf[Long]
     }
     val firstRowId = index.get(internalKey)
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

        currentRow.pointTo(rowBatches(batchNo).rowData, rowOffset + Platform.BYTE_ARRAY_OFFSET, rowlen)
        currentRow.copy()
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
  def multiget(keys: Array[AnyVal]): Iterator[InternalRow] = {
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
  def multigetJoined(keys: Iterator[InternalRow], joiner: UnsafeRowJoiner, rightOutput: Seq[Attribute], joinRightCol: Int): Iterator[InternalRow] = {
    //val keyProjection = UnsafeProjection.create(Seq(rightOutput(joinRightCol)), rightOutput)
    keys.flatMap { right =>
      val key = right.get(joinRightCol, schema(indexCol).dataType)
      get(key.asInstanceOf[AnyVal]).map { left =>
        joiner.join(left.asInstanceOf[UnsafeRow], right.asInstanceOf[UnsafeRow])
      }
    }
  }

  /**
    * a similar multiget, but this one returns joined rows, composed of left + right joined rows
    * we need the projection as a parameter to convert back to unsafe rows
    * @param keys
    * @return
    */
  def multigetBroadcast(keys: Array[InternalRow], joiner: UnsafeRowJoiner, rightOutput: Seq[Attribute], joinRightCol: Int): Iterator[InternalRow] = {
    //val keyProjection = UnsafeProjection.create(Seq(rightOutput(joinRightCol)), rightOutput)
    keys.toIterator.flatMap { right =>
      val key = right.get(joinRightCol, schema(indexCol).dataType)
      get(key.asInstanceOf[AnyVal]).map { left =>
        joiner.join(left.asInstanceOf[UnsafeRow], right.asInstanceOf[UnsafeRow])
      }
    }
  }
}
