package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final int numPages;
    private final ConcurrentHashMap<PageId, Page> pages;
    private final LockManager lockManager;


    // lab4
    // todo：读一下java读写锁，尽管java读写锁是基于thread去锁的，在这个lab无法管用
    private class Lock {
        private TransactionId transactionId;
        private LockType lockType;

        public Lock(TransactionId tid, LockType type) {
            this.transactionId = tid;
            this.lockType = type;
        }
    }

    // page-level lock manager
    private class LockManager {
        ConcurrentHashMap<PageId, Vector<Lock>> lockMap;

        public LockManager() {
            lockMap = new ConcurrentHashMap<>();
        }

        public synchronized boolean acquireLock(TransactionId tid, PageId pid, LockType lockType) {
            // 该pid的page还未上锁
            if (!lockMap.containsKey(pid)) {
                Vector<Lock> vector = new Vector<>();
                vector.add(new Lock(tid, lockType));
                lockMap.put(pid, vector);
                return true;
            }
            Vector<Lock> locks = lockMap.get(pid);
            for (Lock lock : locks) {
                // 该tid已经对此page有上锁
                if (lock.transactionId == tid) {
                    // 该tid申请的lockType已经有上锁
                    if (lock.lockType == lockType) {
                        return true;
                    }
                    // 该tid申请shared_lock，但已上exclusive_lock
                    if (lockType == LockType.SHARED_LOCK) {
                        return true;
                    }
                    // 该tid申请exclusive_lock，但已上shared_lock
                    // 只有自己的tid有在该page上锁，直接升级即可
                    if (locks.size() == 1) {
                        lock.lockType = LockType.EXCLUSIVE_LOCK;
                        return true;
                    }
                    // 还有其他的tid有在该page上锁，无法升级
                    // ps：读优先锁
                    return false;
                }
            }
            // locks中没有该pid, 且locks中已有exclusive_lock
            if (locks.get(0).lockType == LockType.EXCLUSIVE_LOCK) {
                if (locks.size() > 1) {
                    String s = String.format("lockmanager contains more than one lock and at lease one exclusive_lock in page = %s", pid);
                    throw new RuntimeException(s);
                }
                return false;

            }
            // locks中没有该pid，且locks中没有exclusive_lock，且tid申请的是shared_lock
            if (lockType == LockType.SHARED_LOCK) {
                Lock lock = new Lock(tid, LockType.SHARED_LOCK);
                locks.add(lock);
                return true;
            }
            // locks中没有该pid，且locks中没有exclusive_lock，且tid申请的是exclusive_lock
            return false;
        }

        public synchronized boolean releaseLock(TransactionId tid, PageId pid) {
            if (!lockMap.containsKey(pid)) {
//                throw new IllegalArgumentException(String.format("unlocked page: %s", pid));
                return false;
            }
            Vector<Lock> locks = lockMap.get(pid);
            for (Lock lock : locks) {
                if (lock.transactionId == tid) {
                    locks.remove(lock); // 迭代中进行删除，迭代已失效
                    if (locks.size() == 0) {
                        lockMap.remove(pid);
                    }
                    return true;
                }
            }
            // tid not found
            return false;
        }

        public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
            if (!lockMap.containsKey(pid)) {
                return false;
            }
            Vector<Lock> locks = lockMap.get(pid);
            for (Lock lock : locks) {
                if (lock.transactionId == tid) {
                    return true;
                }
            }
            return false;
        }
    }

    private enum LockType {
        SHARED_LOCK, EXCLUSIVE_LOCK
    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        pages = new ConcurrentHashMap<>();
        lockManager = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // 增删改查事务通过getPage方法来获取锁
        LockType lockType = LockType.SHARED_LOCK;
        if (perm != Permissions.READ_ONLY) {
            lockType = LockType.EXCLUSIVE_LOCK;
        }
        boolean acquired = false;
        long start = System.currentTimeMillis();
        long timeout = new Random().nextInt(1000) + 1000;
        while(!acquired) {
            long end = System.currentTimeMillis();
            if (end - start > timeout) {
                throw new TransactionAbortedException();
            }
            acquired = lockManager.acquireLock(tid, pid, lockType);
        }

        if (!pages.containsKey(pid)) {
            // If the page is not present, it should be added to the buffer pool.
            // Attention here.
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbFile.readPage(pid);
            // todo: evictPage IOExeption会导致flush失败
            if (this.pages.size() > numPages) {
                evictPage();
            }
            pages.put(pid, page);
        }
        return pages.get(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        // 为啥要加unsafe...
        lockManager.releaseLock(tid, pid); // ??
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        System.out.println("start insert");
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> modifiedPages = dbFile.insertTuple(tid, t);
        for (Page page : modifiedPages) {
            page.markDirty(true, tid);
            // todo: evictPage IOExeption会导致flush失败
            if (this.pages.size() > numPages) {
                System.out.println("evit start");
                evictPage();
            }
            this.pages.put(page.getId(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> modifiedPages = dbFile.deleteTuple(tid, t);
        for (Page page : modifiedPages) {
            page.markDirty(true, tid);
            // todo: evictPage IOExeption会导致flush失败
            if (this.pages.size() > numPages) {
                evictPage();
            }
            this.pages.put(page.getId(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (Map.Entry<PageId, Page> entry : this.pages.entrySet()) {
            flushPage(entry.getKey());
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        this.pages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = this.pages.get(pid);
        if (page.isDirty() != null) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            dbFile.writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        PageId pageId = new ArrayList<>(this.pages.keySet()).get(0);
        try {
            this.flushPage(pageId);
            this.discardPage(pageId);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
