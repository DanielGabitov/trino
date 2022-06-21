package io.trino.operator.join;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.execution.Lifespan;
import io.trino.operator.Operator;
import io.trino.operator.DriverContext;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlockBuilder;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.*;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static java.util.Objects.requireNonNull;


public class MyJoinOperator implements Operator {

    public static class MyJoinOperatorFactory
            implements OperatorFactory {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final JoinBridgeManager<NestedLoopJoinBridge> joinBridgeManager;
        private final Map<Long, List<Long>> joinMap;
        private final List<Integer> probeChannels;
        private final List<Integer> buildChannels;
        private boolean closed;

        public MyJoinOperatorFactory(int operatorId,
                                     PlanNodeId planNodeId,
                                     Map<Long, List<Long>> joinMap,
                                     JoinBridgeManager<NestedLoopJoinBridge> nestedLoopJoinBridgeManager,
                                     List<Integer> probeChannels,
                                     List<Integer> buildChannels) {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.joinBridgeManager = nestedLoopJoinBridgeManager;
            this.joinBridgeManager.incrementProbeFactoryCount();
            this.joinMap = requireNonNull(joinMap, "joinMap is null");
            //todo channels is broken
            this.probeChannels = ImmutableList.copyOf(requireNonNull(probeChannels, "probeChannels is null"));
            this.buildChannels = ImmutableList.copyOf(requireNonNull(buildChannels, "buildChannels is null"));
        }

        private MyJoinOperatorFactory(MyJoinOperatorFactory other)
        {
            requireNonNull(other, "other is null");
            this.operatorId = other.operatorId;
            this.planNodeId = other.planNodeId;

            this.joinBridgeManager = other.joinBridgeManager;
            this.joinMap = other.joinMap;

            this.probeChannels = ImmutableList.copyOf(other.probeChannels);
            this.buildChannels = ImmutableList.copyOf(other.buildChannels);

            // closed is intentionally not copied
            closed = false;

            joinBridgeManager.incrementProbeFactoryCount();
        }

        @Override
        public Operator createOperator(DriverContext driverContext) {
            checkState(!closed, "Factory is already closed");
            NestedLoopJoinBridge nestedLoopJoinBridge = joinBridgeManager.getJoinBridge(driverContext.getLifespan());

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, NestedLoopJoinOperator.class.getSimpleName());

            joinBridgeManager.probeOperatorCreated(driverContext.getLifespan());
            return new MyJoinOperator(
                    operatorContext,
                    nestedLoopJoinBridge,
                    joinMap,
                    probeChannels,
                    buildChannels,
                    () -> joinBridgeManager.probeOperatorClosed(driverContext.getLifespan()));
        }

        @Override
        public void noMoreOperators() {
            if (closed) {
                return;
            }
            closed = true;
            joinBridgeManager.probeOperatorFactoryClosedForAllLifespans();
        }

        @Override
        public void noMoreOperators(Lifespan lifespan)
        {
            joinBridgeManager.probeOperatorFactoryClosed(lifespan);
        }

        @Override
        public OperatorFactory duplicate() {
            return new MyJoinOperatorFactory(this);
        }
    }

    private final ListenableFuture<NestedLoopJoinPages> nestedLoopJoinPagesFuture;
    private final ListenableFuture<Void> blockedFutureView;

    private final OperatorContext operatorContext;
    private final Runnable afterClose;

    private final int[] probeChannels;
    private final int[] buildChannels;
    private final Map<Long, List<Long>> joinMap;
    private List<Page> buildPages;
    private List<Page> probePages;
    private boolean finishing;
    private boolean finished;
    private boolean closed;

    private MyJoinOperator(OperatorContext operatorContext, NestedLoopJoinBridge joinBridge,
                           Map<Long, List<Long>> joinMap, List<Integer> probeChannels,
                           List<Integer> buildChannels, Runnable afterClose) {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.nestedLoopJoinPagesFuture = joinBridge.getPagesFuture();
        this.joinMap = requireNonNull(joinMap, "joinMap is null");
        blockedFutureView = asVoid(nestedLoopJoinPagesFuture);
        this.probeChannels = Ints.toArray(requireNonNull(probeChannels, "probeChannels is null"));
        this.buildChannels = Ints.toArray(requireNonNull(buildChannels, "buildChannels is null"));
        this.afterClose = requireNonNull(afterClose, "afterClose is null");

        this.probePages = new LinkedList<>();
    }

    private static <T> ListenableFuture<Void> asVoid(ListenableFuture<T> future)
    {
        return Futures.transform(future, v -> null, directExecutor());
    }

    @Override
    public OperatorContext getOperatorContext() {
        return operatorContext;
    }

    @Override
    public boolean needsInput() {
        return true;
    }

    @Override
    public void addInput(Page page) {
        requireNonNull(page, "page is null");
        if (page.getPositionCount() > 0) {
            probePages.add(page);
        }
    }

    @Override
    public Page getOutput() {
        checkState(!finished,  "already finished");
        if (buildPages == null) {
            Optional<NestedLoopJoinPages> nestedLoopJoinPages = tryGetFutureValue(nestedLoopJoinPagesFuture);
            if (nestedLoopJoinPages.isPresent()) {
                buildPages = nestedLoopJoinPages.get().getPages();
            }
        }
        if (!finishing || buildPages == null) return null;
        var pageBuilder = new PageBuilder(probePages, buildPages, joinMap);
        finished = true;
        return pageBuilder.resultPage;
    }

    @Override
    public void finish() {
        finishing = true;
    }

    @Override
    public boolean isFinished() {
        if (finished) {
            close();
        }
        return finished;
    }

    @Override
    public void close() {
        buildPages = null;
        probePages = null;
        // We don't want to release the supplier multiple times, since its reference counted
        if (closed) {
            return;
        }
        closed = true;
        // `afterClose` must be run last.
        afterClose.run();
    }

    private static class PageBuilder {
        private final Page resultPage;

        PageBuilder(List<Page> probePages, List<Page> buildPages, Map<Long, List<Long>> joinMap) {
            if (probePages.isEmpty() || buildPages.isEmpty()) {
                resultPage = new Page(0);
                return;
            }
            int positionCount = 0;
            for (var page: probePages) {
                Block probeBlock = page.getBlock(0);
                for (int pos = 0; pos < page.getPositionCount(); pos++) {
                    var probeId = probeBlock.getLong(pos, 0);
                    positionCount += joinMap.get(probeId).size();
                }
            }
            var buildBlockBuilder = new LongArrayBlockBuilder(null, positionCount);
            var probeBlockBuilder = new LongArrayBlockBuilder(null, positionCount);
            for (var page : probePages) {
                Block probeBlock = page.getBlock(0);
                for (int i = 0; i < page.getPositionCount(); i++) {
                    var probeId= probeBlock.getLong(i, 0);
                    for (var buildId : joinMap.get(probeId)) {
                        probeBlockBuilder.writeLong(probeId);
                        buildBlockBuilder.writeLong(buildId);
                    }
                }
            }
            resultPage = new Page(buildBlockBuilder.build(), probeBlockBuilder.build());
        }

        public Page getPage(){
            return resultPage;
        }
    }
}
