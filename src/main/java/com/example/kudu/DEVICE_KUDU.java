package com.example.kudu;

import org.apache.kudu.Schema;
import org.apache.kudu.client.*;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;


public class DEVICE_KUDU {
    private static AtomicLong id = new AtomicLong(0);

    private static final String KUDU_MASTERS = "10.10.30.200:7051";


    private static final int DEVICE_NUM = 50;
    private static final int BATCH_TOTAL = 10;
    public static final String ID = "id";
    public static final String DEVICE_ID = "device";
    public static final String DEVICE_NAME = "name";
    public static final String ORG_ID = "orgid";
    public static final String DEVICE_KUDU = "impala::default.DEVICE_KUDU";
//    public static final String DEVICE_KUDU = "impala::default.KUDU_WATER_HISTORY_PARTITION_BY_ID";
    public static final String DEVICE_ID_PREFIX = "device";

    private static void insertRows(KuduClient client, String tableName, int deviceNum) throws KuduException {
        // Open the newly-created table and create a KuduSession.
        KuduTable table = client.openTable(tableName);
        KuduSession session = client.newSession();
        // SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND
        // SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC
        SessionConfiguration.FlushMode mode = SessionConfiguration.FlushMode.MANUAL_FLUSH;
        session.setFlushMode(mode);
        if (SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC != mode) {
            session.setMutationBufferSpace((int) 100);
        }
        int batchNum = 0;
        Random random = new Random();
        for (int i = 0; i < deviceNum; i++) {
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            String id = ""+System.currentTimeMillis()+new Random().nextInt(99999999);
            row.addString(ID, id);
            row.addString(DEVICE_ID, DEVICE_ID_PREFIX+i);
            row.addString(DEVICE_NAME, "水表"+i);
            row.addInt(ORG_ID, random.nextInt(50));
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

        System.out.println("Thread:" + Thread.currentThread().getName() + ",rows:" + deviceNum);
    }

    private static void scanTableAndCheckResults(KuduClient client, String tableName, long numRows) throws KuduException {
        KuduTable table = client.openTable(tableName);
        Schema schema = table.getSchema();

        // Scan with a predicate on the 'key' column, returning the 'value' and "added" columns.
        List<String> projectColumns = new ArrayList<>(2);
        projectColumns.add(ID);
        projectColumns.add(DEVICE_ID);
        projectColumns.add(DEVICE_ID);
        projectColumns.add(ORG_ID);
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
                int reading = result.getInt(ORG_ID);
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
        String tableName = DEVICE_KUDU;
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();

        try {
            insertRows(client,tableName,DEVICE_NUM);
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
