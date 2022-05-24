package io.trino.split;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import io.trino.connector.CatalogName;
import io.trino.execution.Lifespan;
import io.trino.metadata.Split;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorPartitionHandle;
import io.trino.spi.connector.MyConnectorSplit;

import java.util.List;
import java.util.Optional;

public class MySplitSource implements SplitSource {

    private final Split mySplit = new Split(getCatalogName(), new MyConnectorSplit(HostAddress.fromParts("127.0.0.1", 8080)), Lifespan.taskWide());
    private final SplitBatch myBatch = new SplitBatch(List.of(mySplit), true);

    private boolean isFinished = false;

    @Override
    public CatalogName getCatalogName() {
        return new CatalogName("MyCatalogName");
    }

    @Override
    public ListenableFuture<SplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, Lifespan lifespan, int maxSize) {
        isFinished = true;
        return Futures.immediateFuture(myBatch);
    }

    @Override
    public void close() {

    }

    @Override
    public boolean isFinished() {
        return isFinished;
    }

    @Override
    public Optional<List<Object>> getTableExecuteSplitsInfo() {
        return Optional.empty();
    }
}
