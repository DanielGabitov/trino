package io.trino.sql.planner.iterative.rule;

import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.Patterns;
import io.trino.sql.planner.plan.MyJoinNode;
import io.trino.sql.planner.plan.TableScanNode;

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

    @Override
    public Result apply(JoinNode node, Captures captures, Context context) {
        if (node.isCrossJoin() || node.getType() != JoinNode.Type.INNER) {
            return Result.empty();
        }
        return Result.ofPlanNode(
                new MyJoinNode(
                        node.getId(),
                        node.getType(),
                        node.getCriteria(),
                        node.getLeftOutputSymbols(),
                        node.getRightOutputSymbols())
        );
    }
}
