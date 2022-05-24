package io.trino.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.sql.planner.Symbol;

import java.util.List;

public class MyJoinNode extends PlanNode {
    private final JoinNode.Type type;
    private final List<JoinNode.EquiJoinClause> criteria;
    private final List<Symbol> leftOutputSymbols;
    private final List<Symbol> rightOutputSymbols;

    @JsonCreator
    public MyJoinNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("type") JoinNode.Type type,
            @JsonProperty("criteria") List<JoinNode.EquiJoinClause> criteria,
            @JsonProperty("leftOutputSymbols") List<Symbol> leftOutputSymbols,
            @JsonProperty("rightOutputSymbols") List<Symbol> rightOutputSymbols) {
        super(id);
        this.type = type;
        this.criteria = criteria;
        this.leftOutputSymbols = leftOutputSymbols;
        this.rightOutputSymbols = rightOutputSymbols;
    }

    @Override
    public List<PlanNode> getSources() {
        return ImmutableList.of();
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
        assert false;
        return this;
    }

    @JsonProperty("type")
    public JoinNode.Type getType()
    {
        return type;
    }

    @JsonProperty("criteria")
    public List<JoinNode.EquiJoinClause> getCriteria() {
        return criteria;
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
