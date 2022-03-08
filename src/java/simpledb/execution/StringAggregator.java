package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbFiledIdx;
    private final Type gbFieldType;
    private final int aggFiledIdx;
    private final Op aggOp;

    private Map<Field, Integer> gbMap; // for COUNT

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbFiledIdx = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aggFiledIdx = afield;
        this.aggOp = what;
        this.gbMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbField = this.gbFiledIdx == NO_GROUPING ? null : tup.getField(gbFiledIdx);
        // type检测
        if (gbField != null && gbField.getType() != this.gbFieldType) {
            throw new IllegalArgumentException(String.format("invalid type of given tuple at %d", gbFiledIdx));
        }
        if (aggOp == Op.COUNT) {
            gbMap.put(gbField, gbMap.getOrDefault(gbField, 0) + 1);
        } else {
            throw new IllegalArgumentException(String.format("unsupported aggregate operator %s", aggOp));
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpIterator() {

            ArrayList<Tuple> tuples = new ArrayList<>();
            TupleDesc td = createTupleDesc();
            Iterator<Tuple> it = null;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                if (aggOp == Op.COUNT) {
                    for (Map.Entry<Field, Integer> entry : gbMap.entrySet()) {
                        Tuple t = new Tuple(td);
                        if (gbFiledIdx == NO_GROUPING) {
                            t.setField(0, new IntField(entry.getValue()));
                        } else {
                            t.setField(0, entry.getKey());
                            t.setField(1, new IntField(entry.getValue()));

                        }
                        tuples.add(t);
                    }
                    it = tuples.iterator();
                } else {
                    throw new IllegalArgumentException(String.format("unsupported aggregate operator %s", aggOp));
                }
            }

            private TupleDesc createTupleDesc() {
                if (gbFiledIdx == NO_GROUPING) {
                    return new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{aggOp.toString()});
                }
                return new TupleDesc(new Type[]{gbFieldType, Type.INT_TYPE}, new String[]{"groupVal",
                        aggOp.toString()});
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return it.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                return it.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return td;
            }

            @Override
            public void close() {
                tuples.clear();
                it = null;
            }
        };
    }

}
