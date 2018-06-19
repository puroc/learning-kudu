package com.example.kudu;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;


public class KUDU_WATER_HISTORY {
    private static AtomicLong id = new AtomicLong(0);
    public static final int MIN_VALUE = 700;
    public static final int MAX_VALUE = 800;
    public static final String YEAR = "2018";
    public static final String MONTH = "09";
    public static final String DATE = "01";
    public static final String HOUR = "15";
    private static final Double DEFAULT_DOUBLE = 12.345;
    //  private static final String KUDU_MASTERS = System.getProperty("kuduMasters", "localhost:7051");
    private static final String KUDU_MASTERS = "10.10.30.200:7051";
//    private static final int THREAD_COUNT = 50;
//    private static final long TOTAL_NUM_ROWS = 100000;
//    private static final int BATCH_TOTAL = 100;

    private static final int THREAD_COUNT = 50;
    private static final long TOTAL_NUM_ROWS = 10000000;
    private static final int BATCH_TOTAL = 100;
    private final static long OPERATION_BATCH_BUFFER = TOTAL_NUM_ROWS / THREAD_COUNT;
    public static final String ID = "id";
    public static final String DEVICE_ID = "device";
    public static final String READING = "reading";
    public static final String TIME = "time";
    public static final String KUDU_WATER_HISTORY = "impala::default.KUDU_WATER_HISTORY";
    public static final String DEVICE_ID_PREFIX = "device";

    private static void createExampleTable(KuduClient client, String tableName) throws KuduException {
        // Set up a simple schema.
        List<ColumnSchema> columns = new ArrayList<>(2);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.STRING)
                .key(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING).nullable(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("added", Type.DOUBLE).defaultValue(DEFAULT_DOUBLE)
                .build());

        Schema schema = new Schema(columns);

        // Set up the partition schema, which distributes rows to different tablets by hash.
        // Kudu also supports partitioning by key range. Hash and range partitioning can be combined.
        // For more information, see http://kudu.apache.org/docs/schema_design.html.
        CreateTableOptions cto = new CreateTableOptions();
        List<String> hashKeys = new ArrayList<>(1);
        hashKeys.add("key");
        int numBuckets = 8;
        cto.addHashPartitions(hashKeys, numBuckets);

        // Create the table.
        client.createTable(tableName, schema, cto);
        System.out.println("Created table " + tableName);
    }

    private static void insertRows(KuduClient client, String threadName, String tableName, long index, String time, long numRows) throws KuduException {
        // Open the newly-created table and create a KuduSession.
        KuduTable table = client.openTable(tableName);
        KuduSession session = client.newSession();
        // SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND
        // SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC
        SessionConfiguration.FlushMode mode = SessionConfiguration.FlushMode.MANUAL_FLUSH;
        session.setFlushMode(mode);
        if (SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC != mode) {
            session.setMutationBufferSpace((int) OPERATION_BATCH_BUFFER);
        }
        int batchNum = 0;
        for (int i = 0; i < numRows; i++) {
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            String id = threadName+"-"+System.currentTimeMillis()+new Random().nextInt(99999999);
            row.addString(ID, id);
            row.addString(DEVICE_ID, threadName);
            int randomValue = Utils.getRandomValue(MIN_VALUE, MAX_VALUE);
            if (i / 2 == 0) {
                randomValue = -1;
            }
            row.addInt(READING, randomValue);
            row.addString(TIME, time);
            session.apply(insert);
            batchNum++;
            if (batchNum == BATCH_TOTAL) {
                //手动提交
                session.flush();
                batchNum = 0;
            }
        }
        //手动提交
        session.flush();

        // Call session.close() to end the session and ensure the rows are
        // flushed and errors are returned.
        // You can also call session.flush() to do the same without ending the session.
        // When flushing in AUTO_FLUSH_BACKGROUND mode (the default mode recommended
        // for most workloads, you must check the pending errors as shown below, since
        // write operations are flushed to Kudu in background threads.
        session.close();
        if (session.countPendingErrors() != 0) {
            System.out.println("errors inserting rows");
            RowErrorsAndOverflowStatus roStatus = session.getPendingErrors();
            RowError[] errs = roStatus.getRowErrors();
            int numErrs = Math.min(errs.length, 5);
            System.out.println("there were errors inserting rows to Kudu");
            System.out.println("the first few errors follow:");
            for (int i = 0; i < numErrs; i++) {
                System.out.println(errs[i]);
            }
            if (roStatus.isOverflowed()) {
                System.out.println("error buffer overflowed: some errors were discarded");
            }
            throw new RuntimeException("error inserting rows to Kudu");
        }

        System.out.println("Thread:" + Thread.currentThread().getName() + ",index:" + index + ",rows:" + numRows);
    }

    private static void scanTableAndCheckResults(KuduClient client, String tableName, long numRows) throws KuduException {
        KuduTable table = client.openTable(tableName);
        Schema schema = table.getSchema();

        // Scan with a predicate on the 'key' column, returning the 'value' and "added" columns.
        List<String> projectColumns = new ArrayList<>(2);
        projectColumns.add(DEVICE_ID);
        projectColumns.add(READING);
        projectColumns.add(TIME);
//        KuduPredicate lowerPred = KuduPredicate.newComparisonPredicate(
//                schema.getColumn("key"),
//                ComparisonOp.GREATER_EQUAL,
//                0);
//        long upperBound = numRows / 2;
//        KuduPredicate upperPred = KuduPredicate.newComparisonPredicate(
//                schema.getColumn("key"),
//                ComparisonOp.LESS,
//                50);
        KuduPredicate equalPred = KuduPredicate.newComparisonPredicate(
                schema.getColumn(DEVICE_ID),
                KuduPredicate.ComparisonOp.EQUAL,
                DEVICE_ID_PREFIX + "1");

        KuduScanner scanner = client.newScannerBuilder(table)
                .setProjectedColumnNames(projectColumns)
//                .addPredicate(lowerPred)
//                .addPredicate(upperPred)
                .addPredicate(equalPred)
                .build();

        // Check the correct number of values and null values are returned, and
        // that the default value was set for the new column on each row.
        // Note: scanning a hash-partitioned table will not return results in primary key order.
        int resultCount = 0;
        long startTime = System.currentTimeMillis();
        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                int reading = result.getInt(READING);
                if (reading > 0) {
                    resultCount++;
                }
            }
        }
        long endTime = System.currentTimeMillis();
        System.out.println("扫描时间：" + (endTime - startTime) + "，符合条件结果数量：" + resultCount);
//        int expectedResultCount = upperBound - lowerBound;
//        if (resultCount != expectedResultCount) {
//            throw new RuntimeException("scan error: expected " + expectedResultCount +
//                    " results but got " + resultCount + " results");
//        }
//        int expectedNullCount = expectedResultCount / 2 + (numRows % 2 == 0 ? 1 : 0);
//        if (nullCount != expectedNullCount) {
//            throw new RuntimeException("scan error: expected " + expectedNullCount +
//                    " rows with value=null but found " + nullCount);
//        }
        System.out.println("Scanned some rows and checked the results," + resultCount);
    }


    static class Task implements Runnable {

        private KuduClient client;
        private String threadName;
        private String tableName;
        private long index;
        private String time;
        private long numRows;

        public Task(KuduClient client, String threadName, String tableName, long index, String time, long numRows) {
            this.client = client;
            this.threadName = threadName;
            this.tableName = tableName;
            this.index = index;
            this.time = time;
            this.numRows = numRows;
        }

        @Override
        public void run() {
            try {
                insertRows(client, threadName, tableName, index, time, numRows);
            } catch (KuduException e) {
                e.printStackTrace();
                deleteTable(tableName, client);
            }
        }
    }

    private static void startInsertWorker(KuduClient client, String tableName, String time, long numRows) {
        long startTime = System.currentTimeMillis();
        //创建线程
        List<Thread> threadList = new ArrayList<Thread>();
        for (int i = 0; i < THREAD_COUNT; i++) {
            long num = 0;
            long index = 0;
            if (numRows % THREAD_COUNT == 0) {
                num = numRows / THREAD_COUNT;
                index = num * i;

            } else {
                if (i == THREAD_COUNT - 1) {
                    num = numRows - i * (numRows / THREAD_COUNT);
                    index = i * (numRows / THREAD_COUNT);
                } else {
                    num = numRows / THREAD_COUNT;
                    index = num * i;
                }
            }

            String threadName = DEVICE_ID_PREFIX + i;
            Thread thread = new Thread(new Task(client, threadName, tableName, index, time, num));
            thread.setName(threadName);
            threadList.add(thread);
        }

        //启动线程
        for (Thread thread : threadList) {
            thread.start();
        }

        //等待线程结束
        for (Thread thread : threadList) {
            try {
                thread.join();
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
        long endTime = System.currentTimeMillis();
        System.out.println("插入时间：" + (endTime - startTime));
    }

    private static void alterTable(String tableName, KuduClient client) throws KuduException {
        // Alter the table, adding a column with a default value.
        // Note: after altering the table, the table needs to be re-opened.
        AlterTableOptions ato = new AlterTableOptions();
        ato.addColumn("added", Type.DOUBLE, DEFAULT_DOUBLE);
        client.alterTable(tableName, ato);
        System.out.println("Altered the table");
    }

    private static void deleteTable(String tableName, KuduClient client) {
        try {
            client.deleteTable(tableName);
            System.out.println("Deleted the table");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                client.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws ParseException {
        System.out.println("-----------------------------------------------");
        System.out.println("Will try to connect to Kudu master(s) at " + KUDU_MASTERS);
        System.out.println("Run with -DkuduMasters=master-0:port,master-1:port,... to override.");
        System.out.println("-----------------------------------------------");
        String tableName = KUDU_WATER_HISTORY;
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();

        try {
            String time = Utils.getDateString(YEAR, MONTH, DATE, HOUR);
//            createExampleTable(client, tableName);
            startInsertWorker(client, tableName, time, TOTAL_NUM_ROWS);
            scanTableAndCheckResults(client, tableName, TOTAL_NUM_ROWS);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
//            deleteTable(tableName, client);
        }

//        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        long time = format.parse("2018-06-01 11:00:00").getTime();
//        System.out.println(format.format(new Date(time)));
    }
}
