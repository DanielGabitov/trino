package io.trino.operator.join;

import io.trino.metadata.Split;
import io.trino.operator.*;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.UpdatablePageSource;
import io.trino.sql.planner.plan.PlanNodeId;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class MyJoinOperator implements SourceOperator {

    public static class MyJoinOperatorFactory
            implements SourceOperatorFactory {
        private final int operatorId;
        private final PlanNodeId sourceId;
        private final List<Integer> probeChannels;
        private final List<Integer> buildChannels;

        public MyJoinOperatorFactory(int operatorId, PlanNodeId sourceId, List<Integer> probeChannels, List<Integer> buildChannels) {
            this.operatorId = operatorId;
            this.sourceId = sourceId;
            this.probeChannels = probeChannels;
            this.buildChannels = buildChannels;
        }

        @Override
        public PlanNodeId getSourceId() {
            return sourceId;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext) {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, sourceId, MyJoinOperator.class.getSimpleName());
            return new MyJoinOperator(operatorContext, sourceId, probeChannels, buildChannels);
        }

        @Override
        public void noMoreOperators() {
        }

        @Override
        public OperatorFactory duplicate() {
            return null;
        }
    }

    private final PlanNodeId sourceId;
    private final OperatorContext operatorContext;
    private final BlockBuilder builder;
    private final Block block;
    private final Page page;

    private final AtomicBoolean needsInput = new AtomicBoolean(true);
    private final AtomicBoolean isFinished = new AtomicBoolean(false);
    private final List<Page> pages = new LinkedList<>();

    private MyJoinOperator(OperatorContext operatorContext, PlanNodeId sourceId, List<Integer> probeChannels, List<Integer> buildChannels) {
        this.sourceId = sourceId;
        this.builder = BIGINT.createBlockBuilder(null, 2);
//        GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using("localhost", 8182));
//        long x = (Long) g.V().hasLabel("supplier").out("link").id().next();
        this.block = builder.build();
        this.page = new Page(block);
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
//        this.probeChannels = Ints.toArray(requireNonNull(probeChannels, "probeChannels is null"));
//        this.buildChannels = Ints.toArray(requireNonNull(buildChannels, "buildChannels is null"));
    }

    @Override
    public PlanNodeId getSourceId() {
        return sourceId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(Split split) {
        return null;
    }

    @Override
    public void noMoreSplits() {

    }

    @Override
    public OperatorContext getOperatorContext() {
        return operatorContext;
    }

    @Override
    public synchronized boolean needsInput() {
        return false;
    }

    @Override
    public synchronized void addInput(Page page) {
        assert false;
    }

    @Override
    public synchronized Page getOutput()
    {
        isFinished.set(true);
        return page;
    }

    @Override
    public synchronized void finish() {
        isFinished.set(true);
    }

    @Override
    public synchronized boolean isFinished() {
        return isFinished.get();
    }
}
