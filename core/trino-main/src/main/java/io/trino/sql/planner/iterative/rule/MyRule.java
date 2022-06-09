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
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.LongLiteral;

import org.testng.collections.Lists;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

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

    public Map<Long, List<Long>> getJoinIds(PlanNode left, PlanNode right, JoinNode.EquiJoinClause clause) {
        try (FileReader fr = new FileReader("data.json");
             BufferedReader br = new BufferedReader(fr)) {
            Map<String, List<String>> result =
                    new ObjectMapper().readValue(br, HashMap.class);
            return result.entrySet().stream().
                    map(entry -> Map.entry(
                            Long.parseLong(entry.getKey()),
                            entry.getValue().stream().map(Long::parseLong).collect(Collectors.toList())
                    )).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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

        List<Expression> leftFilterIds = expressionMap.values().stream().flatMap(List::stream).distinct().collect(Collectors.toList());
        List<Expression> rightFilterIds = Lists.newArrayList(expressionMap.keySet());
        var leftPredicate = new InPredicate(new SymbolReference("orderkey"), new InListExpression(leftFilterIds));
        var rightPredicate = new InPredicate(criteria.getRight().toSymbolReference(), new InListExpression(rightFilterIds));
        var leftFilterNode = new FilterNode(context.getIdAllocator().getNextId(), left, leftPredicate);
        var rightFilterNode = new FilterNode(context.getIdAllocator().getNextId(), right, rightPredicate);

        return Result.ofPlanNode(
                new MyJoinNode(
                        node.getId(),
                        node.getType(),
                        rightFilterNode,
                        leftFilterNode,
                        node.getRightOutputSymbols(),
                        node.getLeftOutputSymbols(),
                        joinMap)
        );
    }
}
