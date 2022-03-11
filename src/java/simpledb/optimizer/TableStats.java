package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    private final int tableId;
    private final int ioCostPerPage;
    private final DbFile dbFile;
    private final TupleDesc td;
    private final int numPages;
    private int numTuples;

    private final Map<Integer, IntHistogram> intHisMap;
    private final Map<Integer, StringHistogram> stringHisMap;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableId = tableid;
        this.ioCostPerPage = ioCostPerPage;
        this.dbFile = Database.getCatalog().getDatabaseFile(tableid);
        this.numPages = ((HeapFile)this.dbFile).numPages();
        this.td = dbFile.getTupleDesc();
        Type[] types = this.td.getFielTypes();
        int numFields = this.td.numFields();
        this.intHisMap = new HashMap<>();
        this.stringHisMap = new HashMap<>();

        int[] mins = new int[numFields];
        for (int i = 0; i < numFields; i++) {
            mins[i] = Integer.MAX_VALUE;
        }
        int[] maxs = new int[numFields];
        for (int i = 0; i < numFields; i++) {
            maxs[i] = Integer.MIN_VALUE;
        }

        // 第一次扫描，获取每个Field的最值
        SeqScan seqScan = new SeqScan(new TransactionId(), tableid);
        this.numTuples = 0;
        try {
            seqScan.open();
            while(seqScan.hasNext()) {
                Tuple t = seqScan.next();
                this.numTuples++;
                for (int i = 0; i < numFields; i++) {
                    if (types[i] == Type.STRING_TYPE) {
                        continue;
                    }
                    mins[i] = Math.min(((IntField)t.getField(i)).getValue(), mins[i]);
                    maxs[i] = Math.max(((IntField)t.getField(i)).getValue(), maxs[i]);
                }
            }
            seqScan.close();
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }
        // 根据刚刚获取的min、max创建各个Field的直方图
        for (int i = 0; i < numFields; i++) {
            if (types[i] == Type.STRING_TYPE) {
                StringHistogram stringHis = new StringHistogram(NUM_HIST_BINS);
                stringHisMap.put(i, stringHis);
            } else {
                // 在NUM_HIST_BINS >> max - min + 1时，数据集的离散程度不够，会出现buckets中数据局部聚簇
                // 导致Estimate the selectivity时有较大误差
                IntHistogram intHis = new IntHistogram(Math.min(NUM_HIST_BINS, maxs[i] - mins[i] + 1), mins[i], maxs[i]);
                intHisMap.put(i, intHis);
            }
        }
        // 第二次扫描,将table数据加入直方图(addValue)
        try {
            seqScan.rewind();
            while (seqScan.hasNext()) {
                Tuple t= seqScan.next();
                for (int i = 0; i < numFields; i++) {
                    if (types[i] == Type.STRING_TYPE) {
                        StringField field = (StringField) t.getField(i);
                        stringHisMap.get(i).addValue(field.getValue());
                    } else {
                        IntField field = (IntField) t.getField(i);
                        intHisMap.get(i).addValue(field.getValue());
                    }
                }
            }
            seqScan.close();
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return this.numPages * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int)(selectivityFactor * numTuples);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        Type type = td.getFieldType(field);
        if (type != constant.getType()) {
            throw new IllegalArgumentException("mismached type");
        }
        if (type == Type.STRING_TYPE) {
            StringField stringField = (StringField) constant;
            return stringHisMap.get(field).estimateSelectivity(op,stringField.getValue());
        } else {
            IntField intField = (IntField) constant;
            return intHisMap.get(field).estimateSelectivity(op,intField.getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return this.numTuples;
    }

}
