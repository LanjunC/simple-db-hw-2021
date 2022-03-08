package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.nio.Buffer;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc tupleDesc;
    // 冗余信息. An ID uniquely identifying this HeapFile.
    // Return the same value for a particular HeapFile.
    // 实际上就是tableId。
    private final int heapFileId;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tupleDesc = td;
        heapFileId = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return heapFileId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    // 该函数的意义：仅会被BufferPool调用，用于直接从disk取Page，而其他地方（外部）获取page只会通过BufferPool
    // Reference from DbFile.java: DbFiles are generally accessed through the buffer pool,
    // rather than directly by operators.
    public Page readPage(PageId pid) {
        // some code goes here
        int tableId = pid.getTableId(); //tableId实际上就是
        int pgNo = pid.getPageNumber();
        int pgSize = BufferPool.getPageSize();
        RandomAccessFile f = null;
        try {
            if (tableId != getId()) {
                // 非法操作
                throw new IllegalArgumentException("invalid tableId: pageId.tableId not equals headFileId");
            }
            f = new RandomAccessFile(file, "r"); // and open
            if ((pgNo + 1) * pgSize > f.length()) {
                throw new IllegalArgumentException(String.format("invalid pageNo %d of tableId %d", pgNo, tableId));
            }
            f.seek(pgNo * pgSize);
            byte[] bytes = new byte[pgSize];
            int read = f.read(bytes, 0, pgSize);
            if (read != pgSize) {
                throw new IllegalArgumentException(String.format("expected %d bytes but read %d", pgSize, read));
            }
            return new HeapPage(new HeapPageId(tableId, pgNo), bytes);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                f.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pageNo = page.getId().getPageNumber();
        if (pageNo > this.numPages()) {
            throw new IllegalArgumentException("invalid pageNumber while writing page");
        }
        RandomAccessFile f = new RandomAccessFile(this.file, "rw");
        f.seek(pageNo * BufferPool.getPageSize());
        f.write(page.getPageData());
        f.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        // Attention
        return (int)(file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        List<Page> res = new ArrayList<>();
        for (int i = 0; i <numPages(); i++) {
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(this.getId(), i), Permissions.READ_WRITE);
            if (page.getNumEmptySlots() == 0) {
                continue;
            }
            page.insertTuple(t);
            res.add(page);
            return res;
        }
        // 无空闲page，需要新建
        BufferedOutputStream bout = new BufferedOutputStream(new FileOutputStream(this.file, true));
        byte[] emptyPageData = HeapPage.createEmptyPageData();
        bout.write(emptyPageData);
        bout.close();
        // 注意file已被追加， numPages()会求得最新结果
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(this.getId(), numPages() - 1),
                Permissions.READ_WRITE);
        page.insertTuple(t);
        res.add(page);
        return res;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        ArrayList<Page> res = new ArrayList<>();
        res.add(page);
        return res;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

    private final class HeapFileIterator implements DbFileIterator {
        private final TransactionId transactionId;
        private Iterator<Tuple> it;
        private int pageNo;

        public HeapFileIterator(TransactionId tid) {
            transactionId = tid;
        }

        private Iterator<Tuple> getPageIterator(int pgNo) throws DbException, TransactionAbortedException {
            if (pgNo < 0 || pgNo >= numPages()) {
                throw new DbException(String.format("invalid pageNo %d while traversing the tuples by iterator", pgNo));
            }
            PageId pid = new HeapPageId(getId(), pgNo);
            HeapPage page = (HeapPage)Database.getBufferPool().getPage(transactionId, pid, Permissions.READ_ONLY);
            return page.iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pageNo = 0;
            it = getPageIterator(pageNo);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (it == null) {
                return false;
            }
            if (!it.hasNext()) {
                while (pageNo < numPages() - 1) {
                    pageNo++;
                    it = getPageIterator(pageNo);
                    if (it.hasNext()) {
                        return true;
                    }
                }
                return false;
            }
            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (it == null) {
                throw new NoSuchElementException("invalid use of iterator");
            }
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            it = null;
        }
    }
}

