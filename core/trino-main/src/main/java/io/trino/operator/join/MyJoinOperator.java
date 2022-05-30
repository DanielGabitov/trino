package io.trino.operator.join;

import io.trino.operator.*;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;


public class MyJoinOperator implements Operator {

    public static class MyJoinOperatorFactory
            implements OperatorFactory {
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
        public Operator createOperator(DriverContext driverContext) {
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
    private Page page;

    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final AtomicBoolean finishing = new AtomicBoolean(false);

    private MyJoinOperator(OperatorContext operatorContext, PlanNodeId sourceId, List<Integer> probeChannels, List<Integer> buildChannels) {
        this.sourceId = sourceId;
        this.builder = BIGINT.createBlockBuilder(null, 2);
        this.block = builder.build();
        this.page = new Page(block);
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");}

    @Override
    public OperatorContext getOperatorContext() {
        return operatorContext;
    }

    @Override
    public boolean needsInput() {
        return !finished.get();
    }

    @Override
    public void addInput(Page page) {
        this.page = page;
    }

    @Override
    public Page getOutput()
    {
        var page = this.page;
        this.page = null;
        if (finishing.get()) {
            finished.set(true);
        }
        return page;
    }

    @Override
    public void finish() {
        finishing.set(true);
    }

    @Override
    public boolean isFinished() {
        return finished.get();
    }
}
