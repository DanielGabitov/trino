package io.trino.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.sql.planner.Symbol;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class MyJoinNode extends PlanNode {
    private final PlanNode left;
    private final PlanNode right;
    private final JoinNode.Type type;
    private final List<Symbol> leftOutputSymbols;
    private final List<Symbol> rightOutputSymbols;

    @JsonCreator
    public MyJoinNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("type") JoinNode.Type type,
            @JsonProperty("left") PlanNode left,
            @JsonProperty("right") PlanNode right,
            @JsonProperty("leftOutputSymbols") List<Symbol> leftOutputSymbols,
            @JsonProperty("rightOutputSymbols") List<Symbol> rightOutputSymbols) {
        super(id);
        this.left = left;
        this.right = right;
        this.type = type;
        this.leftOutputSymbols = leftOutputSymbols;
        this.rightOutputSymbols = rightOutputSymbols;
    }

    @Override
    public List<PlanNode> getSources() {
        return ImmutableList.of(left, right);
    }

    @Override
    public List<Symbol> getOutputSymbols() {
        return ImmutableList.<Symbol>builder()
                .addAll(leftOutputSymbols)
                .addAll(rightOutputSymbols)
                .build();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitMyJoin(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren) {
        checkArgument(newChildren.size() == 2, "expected to be exact 2 sources");
        return new MyJoinNode(getId(), type, newChildren.get(0), newChildren.get(1), leftOutputSymbols, rightOutputSymbols);
    }

    @JsonProperty("left")
    public PlanNode getLeft()
    {
        return left;
    }

    @JsonProperty("right")
    public PlanNode getRight()
    {
        return right;
    }

    @JsonProperty("type")
    public JoinNode.Type getType()
    {
        return type;
    }


    @JsonProperty("leftOutputSymbols")
    public List<Symbol> getLeftOutputSymbols(){
        return leftOutputSymbols;
    }

    @JsonProperty("rightOutputSymbols")
    public List<Symbol> getRightOutputSymbols(){
        return rightOutputSymbols;
    }

}
