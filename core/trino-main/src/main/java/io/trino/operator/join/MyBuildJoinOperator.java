package io.trino.operator.join;

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.spi.Page;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.Optional;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class MyBuildJoinOperator implements Operator {

    public static class MyBuildJoinOperatorFactory
            implements OperatorFactory {

        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final JoinBridgeManager<NestedLoopJoinBridge> nestedLoopJoinBridgeManager;

        private boolean closed;

        public MyBuildJoinOperatorFactory(int operatorId, PlanNodeId planNodeId, JoinBridgeManager<NestedLoopJoinBridge> nestedLoopJoinBridgeManager) {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.nestedLoopJoinBridgeManager = requireNonNull(nestedLoopJoinBridgeManager, "nestedLoopJoinBridgeManager is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext) {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, NestedLoopBuildOperator.class.getSimpleName());
            return new MyBuildJoinOperator(operatorContext, nestedLoopJoinBridgeManager.getJoinBridge(driverContext.getLifespan()));

        }

        @Override
        public void noMoreOperators() {
            if (closed) {
                return;
            }
            closed = true;
        }

        @Override
        public OperatorFactory duplicate() {
            return new MyBuildJoinOperatorFactory(operatorId, planNodeId, nestedLoopJoinBridgeManager);
        }
    }

    private final OperatorContext operatorContext;
    private final NestedLoopJoinBridge nestedLoopJoinBridge;
    private final NestedLoopJoinPagesBuilder nestedLoopJoinPagesBuilder;

    // Initially, probeDoneWithPages is not present.
    // Once finish is called, probeDoneWithPages will be set to a future that completes when the pages are no longer needed by the probe side.
    // When the pages are no longer needed, the isFinished method on this operator will return true.
    private Optional<ListenableFuture<Void>> probeDoneWithPages = Optional.empty();

    public MyBuildJoinOperator(OperatorContext operatorContext, NestedLoopJoinBridge nestedLoopJoinBridge) {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.nestedLoopJoinBridge = requireNonNull(nestedLoopJoinBridge, "nestedLoopJoinBridge is null");
        this.nestedLoopJoinPagesBuilder = new NestedLoopJoinPagesBuilder(operatorContext);
    }

    @Override
    public boolean needsInput() {
        return probeDoneWithPages.isEmpty();
    }

    @Override
    public void addInput(Page page) {
        requireNonNull(page, "page is null");
        checkState(!isFinished(), "Operator is already finished");

        if (page.getPositionCount() == 0) {
            return;
        }

        nestedLoopJoinPagesBuilder.addPage(page);
        operatorContext.recordOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    @Override
    public Page getOutput() {
        return null;
    }

    @Override
    public void finish() {
        if (probeDoneWithPages.isPresent()) {
            return;
        }
        probeDoneWithPages = Optional.of(nestedLoopJoinBridge.setPages(nestedLoopJoinPagesBuilder.build()));
    }

    @Override
    public boolean isFinished() {
        return probeDoneWithPages.map(Future::isDone).orElse(false);
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        return probeDoneWithPages.orElse(NOT_BLOCKED);
    }

    @Override
    public OperatorContext getOperatorContext() {
        return operatorContext;
    }

}
