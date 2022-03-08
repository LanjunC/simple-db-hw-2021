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
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbFiledIdx;
    private final Type gbFieldType;
    private final int aggFiledIdx;
    private final Op aggOp;

    private Map<Field, Integer> gbMap; // MIN MAX SUM COUNT
    private Map<Field, List<Integer>> avgMap; // AVG

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbFiledIdx = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aggFiledIdx = afield;
        this.aggOp = what;
        this.gbMap = new HashMap<>();
        this.avgMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbField = this.gbFiledIdx == NO_GROUPING ? null : tup.getField(gbFiledIdx);
        IntField aggField = (IntField) tup.getField(aggFiledIdx);
        int v = aggField.getValue();
        // type检测
        if (gbField != null && gbField.getType() != this.gbFieldType) {
            throw new IllegalArgumentException(String.format("invalid type of given tuple at %d", gbFiledIdx));
        }
        switch (aggOp) {
            case MIN:
                if (!gbMap.containsKey(gbField)) {
                    gbMap.put(gbField, v);
                } else {
                    gbMap.put(gbField, Math.min(v, gbMap.get(gbField)));
                }
                break;
            case MAX:
                if (!gbMap.containsKey(gbField)) {
                    gbMap.put(gbField, v);
                } else {
                    gbMap.put(gbField, Math.max(v, gbMap.get(gbField)));
                }
                break;
            case COUNT:
                gbMap.put(gbField, gbMap.getOrDefault(gbField, 0) + 1);
                break;
            case SUM:
                gbMap.put(gbField, gbMap.getOrDefault(gbField, 0) + v);
                break;
            case AVG:
                if (!avgMap.containsKey(gbField)) {
                    List<Integer> list = new ArrayList<>();
                    list.add(v);
                    avgMap.put(gbField, list);
                } else {
                    avgMap.get(gbField).add(v);
                }
                break;
            default:
                throw new IllegalArgumentException(String.format("unsupported aggregate operator %s", aggOp));
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpIterator() {

            ArrayList<Tuple> tuples = new ArrayList<>();
            TupleDesc td = createTupleDesc();
            Iterator<Tuple> it = null;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                switch (aggOp) {
                    case MIN:
                    case MAX:
                    case COUNT:
                    case SUM:
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
                        break;
                    case AVG:
                        for (Map.Entry<Field, List<Integer>> entry : avgMap.entrySet()) {
                            Tuple t = new Tuple(td);
                            List<Integer> list = entry.getValue();
                            int sum = 0;
                            for (int i : list) {
                                sum += i;
                            }
                            if (gbFiledIdx == NO_GROUPING) {
                                t.setField(0, new IntField(sum / list.size()));
                            } else {
                                t.setField(0, entry.getKey());
                                t.setField(1, new IntField(sum / list.size()));
                            }
                            tuples.add(t);
                        }
                        it = tuples.iterator();
                        break;
                    default:
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
