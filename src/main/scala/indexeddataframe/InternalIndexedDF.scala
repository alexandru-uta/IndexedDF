package indexeddataframe

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeRow}
import org.apache.spark.sql.types._

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer
import indexeddataframe.RowBatch
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowJoiner
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.unsafe.hash
import org.apache.spark.unsafe.hash.Murmur3_x86_32


/**
  * this the internal data structure that stores a partition of an Indexed Data Frame
  */
class InternalIndexedDF {

  // no. of bits to represent number of batches
  private val NoBitsBatches: Int = 16
  // no. of bits to represent the offsets inside batches
  private val NoBitsOffsets: Int = 18
  // no. of bits on which we represent rowbatch info
  private val NoTotalBits: Int = 64
  // rowbatch size
  private val batchSize: Int = 1 << NoBitsOffsets

  // the index data structure
  private var index:TrieMap[Long, Int] = null

  // the schema of the dataframe
  private var schema:StructType = null
  private var output:Seq[Attribute] = null

  // the column on which the index is computed
  private var indexCol:Int = 0

  // the row batches in which we keep the row data
  private var rowBatches:TrieMap[Int, RowBatch] = null
  private var nRowBatches = 0

  /**
    * init internal data structures of the InternalIndexedDF
    */
  def initialize() = {
    index = new TrieMap[Long, Int]
    rowBatches = new TrieMap[Int, RowBatch]
    rowInfo = new ArrayBuffer[Long]
  }

  /**
    * array that keeps the packed row information
    * in each 64 bit number, for each row we keep
    * a 12 bit batch number
    * a 30 bit previous row id (in case the rows have similar keys
    * a 22 bit offset in the row batch
    */
  private var rowInfo:ArrayBuffer[Long] = null

  // the number of inserted rows in this partition
  private var nRows:Int = 0

  /**
    * function that creates a row batch
    */
  private def createRowBatch() = {
    rowBatches.put(nRowBatches, new RowBatch(batchSize))
    nRowBatches += 1
  }

  /**
    * function that returns a row batch which can fit the current row
    * @param row
    * @return
    */
  private def getBatchForRow(row: Array[Byte]) = {
    if (rowBatches.get(nRowBatches - 1).get.canInsert(row) == false) {
      // if this row cannot fit, create a new batch
      createRowBatch()
    }
    rowBatches.get(nRowBatches - 1).get
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

    createRowBatch()
  }

  /**
    * function that packs the batchNo, rowId and offset of a row into a 64 bit number
    * example:
    * @param batchNo -> 12 bits (4069 batches)
    * @param prevRowId -> 30 bits for the rowId (approx. 1 billion Rows)
    * @param offset -> 22 bits (4 MB batches)
    * => in total we support a partition containing 4096 batches of 4MB == 16GB of max 1B rows
    * @return
    */
  private def packBatchRowIdOffset(batchNo: Integer, prevRowId: Integer, offset: Integer): Long = {
    var result:Long = 0
    result = batchNo.toLong << (NoTotalBits - NoBitsBatches) // put batchNo on the first 12 bits
    result = result | (prevRowId.toLong << NoBitsOffsets) // put rowId on the next 30 bits
    result = result | offset // put offset on the last 22 bits
    result
  }

  /**
    * function that unpacks the batchNo, rowId and offset of a row from a 64 bit number
    * example:
    * @param batchNo -> 12 bits (4069 batches)
    * @param prevRowId -> 30 bits for the rowId (approx. 1 billion Rows)
    * @param offset -> 22 bits (4 MB batches)
    * => in total we support a partition containing 4096 batches of 4MB == 16GB of max 1B rows
    * @return
    */
  private def unpackBatchRowIdOffset(value: Long): (Int, Int, Int) = {
    val batchNo = (value >>> (NoTotalBits - NoBitsBatches)).toInt
    val prevRowId = (((value << NoBitsBatches) >> NoBitsBatches) >>> NoBitsOffsets).toInt
    val offset = ((value << (NoTotalBits - NoBitsOffsets)) >>> (NoTotalBits - NoBitsOffsets)).toInt

    (batchNo, prevRowId, offset)
  }

  /**
    * method that returns the size of a row
    * @param rowId
    */
  private def getSizeOfRow(rowId: Int) = {
    val unpacked = unpackBatchRowIdOffset(rowInfo(rowId))
    val batchNo = unpacked._1
    val prevRowId = unpacked._2
    val offset = unpacked._3

    var result = 0
    if (rowBatches.get(batchNo).get.isLastRow(offset)) {
      result = rowBatches.get(batchNo).get.getLastRowSize
    } else {
      // get info for the next row
      val unpacked2 = unpackBatchRowIdOffset(rowInfo(rowId + 1))
      val nextRowOffset = unpacked2._3
      result = nextRowOffset - offset
    }
    result
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
      // if the key is a string, just get the bytes and hash them
      case StringType => Murmur3_x86_32.hashUnsafeBytes(row.asInstanceOf[UnsafeRow].getString(indexCol).getBytes(),
                        Platform.BYTE_ARRAY_OFFSET, row.asInstanceOf[UnsafeRow].getString(indexCol).length, 42)
      case DoubleType => row.asInstanceOf[UnsafeRow].getDouble(indexCol).toLong
      // fall back to long as default
      case _ => row.asInstanceOf[UnsafeRow].getLong(indexCol)
    }

    // get the current row byte array
    val rowData = row.asInstanceOf[UnsafeRow].getBytes()

    // get the current batch
    val crntBatch = getBatchForRow(rowData)
    val offset = crntBatch.appendRow(rowData)

    // check if the row already exists in the ctrie
    val value = index.get(key)
    if (value != None) {
      // key already exists
      this.index.put(key, this.nRows)
      // update the row info
      this.rowInfo.append(packBatchRowIdOffset(nRowBatches - 1, value.get, offset))
    } else {
      // key does not exist
      this.index.put(key, this.nRows)
      // update the row info
      // we put a -(2^30) here to signal that this is the last row with the same key
      this.rowInfo.append(packBatchRowIdOffset(nRowBatches - 1, ~(-(1<<30)), offset))
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
      val unpacked = unpackBatchRowIdOffset(rowInfo(crntRowId))
      val batchNo = unpacked._1
      val prevRowId = unpacked._2
      val offset = unpacked._3
      val size = getSizeOfRow(crntRowId)

      currentRow.pointTo(rowBatches.get(batchNo).get.rowData, offset + Platform.BYTE_ARRAY_OFFSET, size)

      // last row with this key
      if (~prevRowId == -(1<<30)) this.crntRowId = -1
      else this.crntRowId = prevRowId

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
       // if the key is a string, just get the bytes and hash them
       case StringType => Murmur3_x86_32.hashUnsafeBytes(key.asInstanceOf[String].getBytes(),
              Platform.BYTE_ARRAY_OFFSET, key.asInstanceOf[String].length, 42)

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
    new Iterator[InternalRow]() {
      val currentRow = new UnsafeRow(schema.size)
      var rowIndex = 0

      override def hasNext: Boolean = {
        rowIndex < nRows
      }

      override def next(): InternalRow = {
        val unpacked = unpackBatchRowIdOffset(rowInfo(rowIndex))

        val batchNo = unpacked._1
        val offset = unpacked._3
        val size = getSizeOfRow(rowIndex)

        rowIndex += 1

        currentRow.pointTo(rowBatches.get(batchNo).get.rowData, offset + Platform.BYTE_ARRAY_OFFSET, size)
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
    keys.flatMap { right =>
      val key = right.get(joinRightCol, schema(indexCol).dataType)
      get(key.asInstanceOf[AnyVal]).map { left =>
        joiner.join(left.asInstanceOf[UnsafeRow], right.asInstanceOf[UnsafeRow])
      }
    }
  }

  /**
    * function that returns a shallow copy of the RowBatch
    * but makes a snapshot of the CTrie (such that the new updates are not reflected in the old data structure)
    */
  def getShallowCopy(): InternalIndexedDF = {
    val copy = new InternalIndexedDF
    // take the snapshot for both the index and rowbatches
    copy.index = this.index.snapshot()
    copy.rowBatches = this.rowBatches.snapshot()
    //System.out.println(copy.rowBatches.size() + "!!!!!")
    // all the rest are shallow copies
    copy.schema = this.schema
    copy.output = this.output
    copy.indexCol = this.indexCol
    copy.rowInfo = this.rowInfo
    copy.nRows = this.nRows

    // return the copy
    copy
  }
}
