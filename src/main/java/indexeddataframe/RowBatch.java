package indexeddataframe;

import org.apache.spark.unsafe.Platform;

class RowBatch {
    // 4 MB batch by default
    private int batchSize = 4 * 1024 * 1024;
    // the array that stores the data
    //public byte[] rowData = null;
    public long rowData = -1;
    // the current offset
    private int size = 0;
    private int lastOffset = 0;

    /**
     * constructor for the row batch
     */
    public RowBatch(int batchSize) {
        this.batchSize = batchSize;
        //rowData = new byte[batchSize];
        rowData = Platform.allocateMemory(batchSize);
        Platform.setMemory(rowData, (byte)0, batchSize);
    }

    /**
     * function that appends a row and returns the offset at which it can be found
     * @param crntRow
     */
    public int appendRow(byte[] crntRow) {
        //System.arraycopy(crntRow, 0, rowData, size, crntRow.length);
        Platform.copyMemory(crntRow, Platform.BOOLEAN_ARRAY_OFFSET, null, rowData + size, crntRow.length);

        int returnedOffset = size;
        lastOffset = size;
        size += crntRow.length;
        return returnedOffset;
    }

    /**
     * function that appends a row and returns the offset at which it can be found
     * @param rowSize
     */
    public int updateAppendedRowSize(int rowSize) {
        //System.arraycopy(crntRow, 0, rowData, size, crntRow.length);
        //Platform.copyMemory(crntRow, Platform.BOOLEAN_ARRAY_OFFSET, null, rowData + size, crntRow.length);

        int returnedOffset = size;
        lastOffset = size;
        size += rowSize;
        return returnedOffset;
    }

    /**
     * function that gets a row from a given offset and with a given length
     * @param offset
     * @param len
     * @return
     */
    public byte[] getRow(int offset, int len) {
        if (len < 0 || offset > size) {
            // incorrect params
            return null;
        }
        byte[] row = new byte[len];
        //System.arraycopy(rowData, offset, row, 0, len);
        Platform.copyMemory(null, rowData + offset, row, Platform.BYTE_ARRAY_OFFSET, len);
        return row;
    }

    /**
     * function that checks whether there is enough space to insert a new row
     * @param rowSize: the size of the row to be inserted
     * @return
     */
    public boolean canInsert(int rowSize) {
        if (size + rowSize >= batchSize)
            return false;
        return true;
    }

    public boolean isLastRow(int offset) {
        return (offset == lastOffset);
    }

    public int getLastRowSize() {
        return size - lastOffset;
    }

    public int getCurrentOffset() { return size; }

    public long getCurrentPointer() { return rowData; }
}
