package indexeddataframe

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types._
import scala.collection.concurrent.TrieMap
import indexeddataframe.RowBatch
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateUnsafeRowJoiner, UnsafeRowJoiner}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.hash.Murmur3_x86_32


/**
  * this the internal data structure that stores a partition of an Indexed Data Frame
  */
class InternalIndexedDF {

  /* the following info decides how to pack the backward crawling
    pointers inside a unsigned 64 bit integer
   */
  // no. of bits to represent number of batches
  // this accounts for 4B batches
  private val NoBitsBatches: Int = 22
  // no. of bits to represent the offsets inside batches
  // this accounts for 4MB batches
  private val NoBitsOffsets: Int = 32
  // no. of bits on which we represent rowbatch info
  private val NoTotalBits: Int = 64
  // the number of bits to represent the row size inside a packed 64 bit integer
  // in the current setup we allow for 1KB rows, but this can be configured
  private val NoRowSizeBits: Int = NoTotalBits - NoBitsOffsets - NoBitsBatches

  // rowbatch size
  private val batchSize: Int = (1 * 1024 * 1024)

  // the index data structure
  private var index:TrieMap[Long, Long] = null

  // the schema of the dataframe
  private var schema:StructType = null
  private var output:Seq[Attribute] = null

  // the column on which the index is computed
  private var indexCol:Int = 0
  // the number of columns
  private var nColumns:Int = 0

  // the row batches in which we keep the row data
  private var rowBatches:TrieMap[Int, RowBatch] = null
  private var nRowBatches = 0

  // projection that adds the #prev column for the backward chasing pointers
  private var backwardPointerJoiner:UnsafeRowJoiner = null
  // projection for converting to unsafe rows, in case the input rows are not unsafe rows
  private var convertToUnsafe:UnsafeProjection = null

  /**
    * init internal data structures of the InternalIndexedDF
    */
  def initialize() = {
    index = new TrieMap[Long, Long]
    rowBatches = new TrieMap[Int, RowBatch]
  }

  // the number of rows of the InternalIndexed DF partition
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
    * @param size: the size of the row
    * @return
    */
  private def getBatchForRowSize(size: Int) = {
    if (rowBatches.get(nRowBatches - 1).get.canInsert(size) == false) {
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
      this.nColumns += 1
    }
    this.indexCol = columnNo
    this.output = output

    // create a simple schema for a row that just contains the #prev field
    var rightSchema = new StructType()
    val rightField = new StructField("prev", LongType)
    rightSchema = rightSchema.add(rightField)
    // generate code for joining the inserted rows with the #prev field
    // this basically means adding one column
    backwardPointerJoiner = GenerateUnsafeRowJoiner.create(schema, rightSchema)
    // initialize the unsafe projection
    convertToUnsafe = UnsafeProjection.create(schema)

    createRowBatch()
  }

  /**
    * function that packs the batchNo, rowId and offset of a row into a 64 bit number
    * example:
    * @param batchNo -> the batch id
    * @param offset -> the offset within a batch
    * @param size -> the size of the row
    * @return the packed 64 bit integer
    */
  private def packBatchRowIdOffset(batchNo: Integer, offset: Integer, size: Integer): Long = {
    var result:Long = 0
    result = batchNo.toLong << (NoTotalBits - NoBitsBatches) // put batchNo on the first 12 bits
    result = result | (offset.toLong << NoRowSizeBits) // put offset on the next 30 bits
    result = result | size // put size on the last 22 bits
    result
  }

  /**
    * function that unpacks the batchNo, rowId and offset of a row from a 64 bit number
    * example:
    * @param batchNo -> id of the rowbatch
    * @param offset -> offset within rowbatch
    * @param size -> size of row
    * @return the unpacked integer as a tuple of (batchNo, offset, size)
    */
  private def unpackBatchRowIdOffset(value: Long): (Int, Int, Int) = {
    val batchNo = (value >>> (NoTotalBits - NoBitsBatches)).toInt
    val offset = (((value << NoBitsBatches) >>> NoBitsBatches) >>> NoRowSizeBits).toInt
    val size = ((value << (NoTotalBits - NoRowSizeBits)) >>> (NoTotalBits - NoRowSizeBits)).toInt

    (batchNo, offset, size)
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

    // create an unsafe row that contains just the #prev field
    val prevRow = new UnsafeRow(1)
    // the row's byte array should have 16 bytes
    var prevByteArray = new Array[Byte](16)
    // clean the first 8 bytes (the null bitset field of the unsafe row)
    for (i <- 0 to 7) prevByteArray(i) = 0
    prevRow.pointTo(prevByteArray, 16)

    // join the current row to be inserted with the #prev field
    // but check first whether it is already an unsafe row
    // if not, we need to convert it
    val resultRow = if (row.isInstanceOf[UnsafeRow])
      backwardPointerJoiner.join(row.asInstanceOf[UnsafeRow], prevRow)
    else
      backwardPointerJoiner.join(convertToUnsafe(row), prevRow)

    // get the row size
    val rowDataSize = resultRow.getSizeInBytes

    // get the current batch
    val crntBatch = getBatchForRowSize(rowDataSize)
    val offset = crntBatch.getCurrentOffset()

    // build backward crawling pointer
    val cTriePointer = packBatchRowIdOffset(nRowBatches - 1, offset, rowDataSize)

    // check if the row already exists in the ctrie
    val value = index.get(key)
    if (value != None) {
      resultRow.setLong(this.nColumns, value.get)
    } else {
      // with 0xff represent the "end of line" in pointer chasing
      resultRow.setLong(this.nColumns, 0xffffffffffffffffL)
    }
    // store the pointer in the cTrie
    this.index.put(key, cTriePointer)
    // put the data in the rowbatch
    crntBatch.appendRow(resultRow.getBytes)
    // increase the number of rows
    this.nRows += 1

    // just a sanity check
    val res = unpackBatchRowIdOffset(cTriePointer)
    assert(nRowBatches - 1 == res._1 && offset == res._2 && rowDataSize == res._3)
  }

  /**
    * method that appends a list of InternalRow
    * @param rows
    */
  def appendRows(rows: Iterator[InternalRow]) = {
    rows.foreach( row => {
      appendRow(row)
    })
    //System.out.println("!!!! " + min + " " + max)
  }

  /**
    * iterator returned by the get function on the indexed data frame
    * @rowPointer: parameter that contains the #prev row pointer
    */
  class RowIterator(rowPointer: Long) extends Iterator[InternalRow] {
    // unsafeRow object that points to the byte array representing its data
    private val currentRow = new UnsafeRow(schema.size)
    private var crntRowPointer = rowPointer

    def hasNext(): Boolean = {
      var ret = false
      if (crntRowPointer != 0xffffffffffffffffL) {
        ret = true
      }
      ret
    }
    def next(): InternalRow = {
      // unpack the pointer
      val unpacked = unpackBatchRowIdOffset(crntRowPointer)
      val batchNo = unpacked._1
      val offset = unpacked._2
      val size = unpacked._3
      // get the row data
      currentRow.pointTo(rowBatches.get(batchNo).get.rowData, offset + Platform.BYTE_ARRAY_OFFSET, size)
      // update the current row pointer
      crntRowPointer = currentRow.getLong(nColumns)
      // return the current row
      currentRow
    }
  }

  /**
    * function that performs lookups in the indexed data frame
    * returns an iterator of rows
   */
   def get(key: Any): Iterator[InternalRow] = {
     // check the type of the key and transform to long
     val internalKey = key match  {
       case _: Long => key.asInstanceOf[Long]
       case _: Int => key.asInstanceOf[Int].toLong
       // if the key is a string, just get the bytes and hash them
       case _: String => Murmur3_x86_32.hashUnsafeBytes(key.asInstanceOf[String].getBytes(),
              Platform.BYTE_ARRAY_OFFSET, key.asInstanceOf[String].length, 42)
       case _: Double => key.asInstanceOf[Double].toLong
       // fall back to long as default
       case _ => key.asInstanceOf[Long]
     }
     //println("key = " + key)
     val rowPointer = index.get(internalKey)
     var ret: RowIterator = null
     if (rowPointer != None) ret = new RowIterator(rowPointer.get)
     else ret = new RowIterator(0xffffffffffffffffL)
     ret
  }

  /**
    * iterator function imposed by the RDD interface
    */
  def iterator(): Iterator[InternalRow] = {
    index.iterator.flatMap{ pair =>
      get(pair._1.asInstanceOf[AnyVal]).map {
        row => row.copy()
      } }
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
    keys.toIterator.flatMap { key =>
      get(key).map { row => row.copy() }
    }
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
    * function that returns a snapshot of the InternalIndexedDF partition
    * but makes a snapshot of the CTries (such that the new updates are not reflected in the old data structure)
    * this function is efficient because there is no data copying, we use the cTrie
    * to do all the smart heavy-lifting with the snapshotting capability
    */
  def getSnapshot(): InternalIndexedDF = {
    val copy = new InternalIndexedDF
    // take the snapshot for both the index and rowbatches
    copy.index = this.index.snapshot()
    copy.rowBatches = this.rowBatches.snapshot()
    // for the copy, go to the next row batch
    // otherwise, the two copies will write on the same last rowbatch
    copy.nRowBatches = this.nRowBatches
    copy.createRowBatch()
    // copy all the other variables
    copy.schema = this.schema
    copy.output = this.output
    copy.indexCol = this.indexCol
    copy.nColumns = this.nColumns
    copy.nRows = this.nRows
    // initialize the projections
    var rightSchema = new StructType()
    val rightField = new StructField("prev", LongType)
    rightSchema = rightSchema.add(rightField)
    copy.backwardPointerJoiner = GenerateUnsafeRowJoiner.create(schema, rightSchema)
    copy.convertToUnsafe = UnsafeProjection.create(schema)

    // return the copy
    copy
  }
}
