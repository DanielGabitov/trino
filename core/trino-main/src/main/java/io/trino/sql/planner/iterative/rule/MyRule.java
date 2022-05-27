package io.trino.sql.planner.iterative.rule;

import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.Patterns;
import io.trino.sql.planner.plan.MyJoinNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.tree.*;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.plan.Patterns.Join.left;
import static io.trino.sql.planner.plan.Patterns.Join.right;
import static io.trino.sql.planner.plan.Patterns.tableScan;

public class MyRule implements Rule<JoinNode> {

    private static final Capture<TableScanNode> LEFT_TABLE_SCAN = newCapture();
    private static final Capture<TableScanNode> RIGHT_TABLE_SCAN = newCapture();


    private static final Pattern<JoinNode> PATTERN =
            Patterns.join()
                    .with(left().matching(tableScan().capturedAs(LEFT_TABLE_SCAN)))
                    .with(right().matching(tableScan().capturedAs(RIGHT_TABLE_SCAN)));

    private final Metadata metadata;

    public MyRule(Metadata metadata) {
        this.metadata = metadata;
    }

    @Override
    public Pattern<JoinNode> getPattern() {
        return PATTERN;
    }

    public List<Expression> getJoinIds(PlanNode left, PlanNode right, JoinNode.EquiJoinClause clause) {
        return List.of(new LongLiteral("1"), new LongLiteral("3"));
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context) {
        if (node.isCrossJoin() || node.getType() != JoinNode.Type.INNER) {
            return Result.empty();
        }
        checkArgument(node.getLeft() instanceof GroupReference);
        checkArgument(node.getRight() instanceof GroupReference);
        checkArgument(node.getCriteria().size() == 1);
        var criteria = node.getCriteria().get(0);
        checkArgument(!criteria.getMyLeft().isEmpty());
        checkArgument(!criteria.getMyRight().isEmpty());

        var leftGroupId = ((GroupReference) node.getLeft()).getGroupId();
        var rightGroupId = ((GroupReference) node.getRight()).getGroupId();

        var memo = context.getMemo();
        var left = memo.getNode(leftGroupId);
        var right = memo.getNode(rightGroupId);

        checkArgument(left instanceof TableScanNode);
        checkArgument(right instanceof TableScanNode);

        List<Expression> filterIds = getJoinIds(left, right, criteria);
        var leftPredicate = new InPredicate(criteria.getLeft().toSymbolReference(), new InListExpression(filterIds));
        var rightPredicate = new InPredicate(criteria.getRight().toSymbolReference(), new InListExpression(filterIds));

        var leftFilterNode = new FilterNode(context.getIdAllocator().getNextId(), left, leftPredicate);
        var rightFilterNode = new FilterNode(context.getIdAllocator().getNextId(), right, rightPredicate);

        return Result.ofPlanNode(
                new MyJoinNode(
                        node.getId(),
                        node.getType(),
                        leftFilterNode,
                        rightFilterNode,
                        node.getLeftOutputSymbols(),
                        node.getRightOutputSymbols())
        );
    }
}
