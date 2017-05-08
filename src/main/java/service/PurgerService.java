package service;

import config.PurgerConfig;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.example.generated.BulkDeleteProtos;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.snapshot.ExportSnapshotException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static config.PurgerConfig.BATCH_SIZE_KEY;
import static config.PurgerConfig.DATA_TABLE_KEY;
import static config.PurgerConfig.RETENTION_KEY;

public class PurgerService {
    private static Logger log = Logger.getLogger(PurgerService.class);
    private static int METRIC_WIDTH = 5;
    private static int TIMESTAMP_WIDTH = 4;
    private Table dataTable;
    private Connection connection;
    private ExecutorService executorService;
    private PurgerConfig purgerConfig;
    private UidFetcher uidFetcher;
    private int retentionInDays;
    private int batchSize;

    public PurgerService(Connection connection, PurgerConfig config) throws IOException {
        this.connection = connection;
        this.purgerConfig = config;
        this.retentionInDays = Integer.valueOf((String) config.getConfig(RETENTION_KEY));
        this.batchSize = Integer.valueOf((String) config.getConfig(BATCH_SIZE_KEY));
        this.dataTable = connection.getTable(TableName.valueOf(Bytes.toBytes((String) config.getConfig(DATA_TABLE_KEY))));
        executorService = Executors.newFixedThreadPool(batchSize);
    }

    public void start() throws IOException, ExecutionException, InterruptedException {
        uidFetcher = new UidFetcher(connection, purgerConfig);

        log.info("Starting purge");

        for(List<Cell> metricUids = uidFetcher.getNext(); metricUids.size() != 0; metricUids=uidFetcher.getNext()) {

            List<Future<Long>> futures = new ArrayList<>();

            for(Cell metricUid : metricUids) {

                Scan scan = new Scan();
                scan.addFamily(Bytes.toBytes("t"));
                scan.setStartRow(metricUid.getRow());
                scan.setStopRow(getStopRow(metricUid));

                Batch.Call<BulkDeleteProtos.BulkDeleteService, BulkDeleteProtos.BulkDeleteResponse> callable = new Batch.Call<BulkDeleteProtos.BulkDeleteService, BulkDeleteProtos.BulkDeleteResponse>() {
                    ServerRpcController controller = new ServerRpcController();
                    BlockingRpcCallback<BulkDeleteProtos.BulkDeleteResponse> rpcCallback = new BlockingRpcCallback<>();


                    @Override
                    public BulkDeleteProtos.BulkDeleteResponse call(BulkDeleteProtos.BulkDeleteService bulkDeleteService) throws IOException {
                        BulkDeleteProtos.BulkDeleteRequest.Builder builder = BulkDeleteProtos.BulkDeleteRequest.newBuilder();
                        builder.setScan(ProtobufUtil.toScan(scan));
                        builder.setDeleteType(BulkDeleteProtos.BulkDeleteRequest.DeleteType.ROW);
                        builder.setRowBatchSize(100);
                        bulkDeleteService.delete(controller, builder.build(), rpcCallback);
                        return rpcCallback.get();
                    }
                };

                futures.add(executorService.submit(new Callable<Long>() {
                    @Override
                    public Long call() throws Exception {
                        Long deleted = 0l;
                        try {
                            Map<byte[], BulkDeleteProtos.BulkDeleteResponse> result = dataTable.coprocessorService(BulkDeleteProtos.BulkDeleteService.class, scan.getStartRow(), scan.getStopRow(), callable);
                            for (BulkDeleteProtos.BulkDeleteResponse response : result.values()) {
                                deleted += response.getRowsDeleted();
                            }
                        } catch (Throwable throwable) {
                            log.error(Bytes.toStringBinary(metricUid.getRowArray()) + "; deletion failed");
                            throw new Exception(throwable);
                        }

                        log.info(Bytes.toStringBinary(metricUid.getRowArray()) + "; deleted rows: " + deleted);
                        return deleted;
                    }
                }));

                for(Future future : futures) {
                    future.get();
                }
            }
        }
        log.info("Purge finished");
        uidFetcher.close();
    }

    private byte[] getStopRow(Cell metricUid) {
        byte[] stopKey = new byte[METRIC_WIDTH + TIMESTAMP_WIDTH + 1];

        System.arraycopy(metricUid.getRow(), 0, stopKey, 0, METRIC_WIDTH);

        int lastTime = (int) (DateUtils.addDays(new Date(), -retentionInDays).getTime()/1000l);
        setInt(stopKey, lastTime, METRIC_WIDTH);

        stopKey[METRIC_WIDTH + TIMESTAMP_WIDTH] = (byte) 0;
        return stopKey;
    }

    public static void setInt(final byte[] b, final int n, final int offset) {
        b[offset + 0] = (byte) (n >>> 24);
        b[offset + 1] = (byte) (n >>> 16);
        b[offset + 2] = (byte) (n >>>  8);
        b[offset + 3] = (byte) (n >>>  0);
    }
}
