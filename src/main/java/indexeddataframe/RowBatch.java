package indexeddataframe;

class RowBatch {
    // 10 MB batch
    private static final int batchSize = 1024 * 1024;
    // the array that stores the data
    public byte[] rowData = null;
    // the current offset
    private int size = 0;

    /**
     * constructor for the row batch
     */
    public RowBatch() {
        rowData = new byte[batchSize];
    }

    /**
     * function that appends a row and returns the offset at which it can be found
     * @param crntRow
     */
    public int appendRow(byte[] crntRow) {
        if (rowData == null) {
            // the row batch was not initialized
            return -1;
        }
        System.arraycopy(crntRow, 0, rowData, size, crntRow.length);
        int returnedOffset = size;
        size += crntRow.length;
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
        System.arraycopy(rowData, offset, row, 0, len);
        return row;
    }

    /**
     * function that checks whether there is enough space to insert a new row
     * @param crntRow
     * @return
     */
    public boolean canInsert(byte[] crntRow) {
        if (size + crntRow.length >= batchSize)
            return false;
        return true;
    }
}
