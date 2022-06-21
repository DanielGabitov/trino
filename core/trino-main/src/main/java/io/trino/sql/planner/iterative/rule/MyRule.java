package io.trino.sql.planner.iterative.rule;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.LongLiteral;

import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.testng.collections.Lists;


import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.plan.Patterns.Join.left;
import static io.trino.sql.planner.plan.Patterns.Join.right;
import static io.trino.sql.planner.plan.Patterns.tableScan;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;


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

    public Map<Long, List<Long>> getJoinIds(PlanNode left, PlanNode right, JoinNode.EquiJoinClause clause) {
        GraphTraversalSource g = traversal().withRemote(
                DriverRemoteConnection.using("localhost", 8182));
        var paths = g.V().hasLabel("left").out().path().by("left_id").by("right_id").toList();
        Map<Long, List<Long>> result = new HashMap<>();
        for (var path : paths) {
            var left_id = ((Number) path.get(0)).longValue();
            var right_id = ((Number) path.get(1)).longValue();
            if (result.containsKey(left_id)) {
                result.get(left_id).add(right_id);
            } else {
                result.put(left_id, new LinkedList<>(List.of(right_id)));
            }
        }
        return result;
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

        var joinMap = getJoinIds(left, right, criteria);
        var expressionMap =
                joinMap.entrySet().stream().
                        map(x -> Map.entry(
                                (Expression) new LongLiteral(x.getKey().toString()),
                                x.getValue().stream().
                                        map(y -> (Expression) new LongLiteral(y.toString())).
                                        collect(Collectors.toList()))
                ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        List<Expression> leftFilterIds = Lists.newArrayList(expressionMap.keySet());
        List<Expression> rightFilterIds = expressionMap.values().stream().flatMap(List::stream).distinct().collect(Collectors.toList());
        var leftPredicate = new InPredicate(criteria.getLeft().toSymbolReference(), new InListExpression(leftFilterIds));
        var rightPredicate = new InPredicate(criteria.getRight().toSymbolReference(), new InListExpression(rightFilterIds));
        var leftFilterNode = new FilterNode(context.getIdAllocator().getNextId(), left, leftPredicate);
        var rightFilterNode = new FilterNode(context.getIdAllocator().getNextId(), right, rightPredicate);

        return Result.ofPlanNode(
                new MyJoinNode(
                        node.getId(),
                        node.getType(),
                        leftFilterNode,
                        rightFilterNode,
                        node.getLeftOutputSymbols(),
                        node.getRightOutputSymbols(),
                        joinMap)
        );
    }
}
