package io.trino.spi.connector;

import io.trino.spi.type.Type;

import java.util.List;
import java.util.function.ToIntFunction;

public class MyConnectorNodePartitioningProvider implements ConnectorNodePartitioningProvider{
    @Override
    public ConnectorBucketNodeMap getBucketNodeMap(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle) {
        return ConnectorBucketNodeMap.createBucketNodeMap(1);
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle) {
        return value -> 0;
    }

    @Override
    public BucketFunction getBucketFunction(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorPartitioningHandle partitioningHandle, List<Type> partitionChannelTypes, int bucketCount) {
        return null;
    }
}
