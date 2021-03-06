package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import jdk.internal.jshell.tool.resources.l10n;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
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


    private int ioPerPage;
    private Object[] hists;
    private int totalTups;

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


        // get the DbFile for the table in question
        DbFile f = Database.getCatalog().getDatabaseFile(tableid);
        // DbFile iterator needs a tid
        TransactionId tid = new TransactionId();
        
        TupleDesc td = f.getTupleDesc();
        
        int numFields = td.numFields();
        hists = new Object[numFields];
        makeHistograms(f, tid, td, hists);
        fillHistograms(f, tid, td, hists);

        ioPerPage = ((HeapFile) f).numPages() * ioCostPerPage;
        

    }

    // could be int or string; if int, we need to get the mins and maxes.
    // if str, we call stringHistogram, which gets the mins and maxes itself.
    private void makeHistograms(DbFile f, TransactionId tid, TupleDesc td, Object[] hists) {
        DbFileIterator iter = f.iterator(tid);
        // storing hists
        
        int numFields = td.numFields();
        
        try {
            iter.open();

            
            // just using arrays because these are intuitive to me
            int[] min = new int[numFields];
            int[] max = new int[numFields]; 
            for(int i = 0; i<numFields; i++) {
                // set up initial values for greedy algo
                min[i] = Integer.MAX_VALUE;
                max[i] = Integer.MIN_VALUE;
            }


            while (iter.hasNext()){
                Tuple next = iter.next();
                totalTups++;

                for(int i = 0; i< numFields; i++) {
                    if (td.getFieldType(i) == Type.INT_TYPE) {
                        Field field = next.getField(i);
                        // must be cast as intfield to get the value, and we can do this because we know
                        // from above that it's intType
                        int value = ((IntField) field).getValue();
                        if (value < min[i]) {
                            min[i] = value;
                        }
                        else if (value > max[i]) {
                            max[i] = value;
                        }

                    } // we don't do anything if it's a string
                }
                
            }

            iter.close();

            for(int i = 0; i< numFields; i++) {
                if(td.getFieldType(i) == Type.INT_TYPE) {
                    hists[i] = new IntHistogram(NUM_HIST_BINS, min[i], max[i]);
                }
                else {
                    hists[i] = new StringHistogram(NUM_HIST_BINS);
                }
            }

        } catch (Exception e) { 
            e.printStackTrace();
        }
    }

    private void fillHistograms(DbFile f, TransactionId tid, TupleDesc td, Object[] hists) {
        DbFileIterator iter = f.iterator(tid);

        int numFields = td.numFields();
        try {
            iter.open();

            while (iter.hasNext()){
                Tuple next = iter.next();

                for(int i = 0; i< numFields; i++) {
                    if (td.getFieldType(i) == Type.INT_TYPE) {
                        Field field = next.getField(i);
                        // must be cast as intfield to get the value, and we can do this because we know
                        // from above that it's intType
                        int value = ((IntField) field).getValue();
                        IntHistogram h = (IntHistogram) hists[i];
                        h.addValue(value);
                
                    } else { // string hist
                        Field field = next.getField(i);
                        String value = ((StringField) field).getValue();
                        StringHistogram h = (StringHistogram) hists[i];
                        h.addValue(value);
                    }
                }
                
            }

            iter.close();

        } catch (Exception e) { 
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
        return ioPerPage;
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
        return (int) (totalTups * selectivityFactor);
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
        Object hist = hists[field];
        Type t = constant.getType();
        if(t == Type.INT_TYPE) {
            IntHistogram h = ((IntHistogram) hist);
            int value = ((IntField) constant).getValue();
            return h.estimateSelectivity(op, value);
        } else {
            StringHistogram h = ((StringHistogram) hist);
            String value = ((StringField) constant).getValue();
            return h.estimateSelectivity(op, value);
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return this.totalTups;
    }

}
