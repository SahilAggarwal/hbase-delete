package service;

import config.PurgerConfig;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static config.PurgerConfig.*;

public class UidFetcher {
    private static Logger log = Logger.getLogger(UidFetcher.class);
    private Table uidTable;
    private int batchSize;
    private ResultScanner scanner;

    public UidFetcher(Connection connection, PurgerConfig config) throws IOException {

        uidTable = connection.getTable(TableName.valueOf(Bytes.toBytes((String) config.getConfig(UID_TABLE_KEY))));
        batchSize = Integer.valueOf((String) config.getConfig(BATCH_SIZE_KEY));

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("name"), Bytes.toBytes("metrics"));

        try {
            scanner = uidTable.getScanner(scan);
        } catch (IOException e) {
            log.error("Failed to create scanner on table: " + uidTable.getName(), e);
        }
    }

    public List<Cell> getNext() throws IOException {
        List<Cell> metricUids = new ArrayList<>();

        for(int i=0;i<batchSize;i++) {
            Result result = scanner.next();
            if(result != null) {
                List<Cell> cells = result.getColumnCells(Bytes.toBytes("name"), Bytes.toBytes("metrics"));
                for (Cell cell : cells) {
                    metricUids.add(cell);
                }
            }
        }
        return metricUids;
    }

    public void close() {
        scanner.close();
    }
}
