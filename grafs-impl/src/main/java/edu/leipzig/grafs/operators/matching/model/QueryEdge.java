package edu.leipzig.grafs.operators.matching.model;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Element;
import java.util.ArrayList;
import java.util.Collection;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.s1ck.gdl.model.comparables.ComparableExpression;
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.comparables.PropertySelector;
import org.s1ck.gdl.model.predicates.Predicate;
import org.s1ck.gdl.model.predicates.expressions.Comparison;

public class QueryEdge extends Edge implements HasPredicate {

    Collection<Predicate> selfPredicates;
    int order;
    private ArrayList<String> variables = new ArrayList<>();


    public QueryEdge(GradoopId id, String label, GradoopId sourceId, GradoopId targetId, Properties properties, GradoopIdSet graphIds) {
        super(id, label, sourceId, targetId, properties, graphIds);
        selfPredicates = new ArrayList<>();
        variables = new ArrayList<>();
    }

    public QueryEdge(GradoopId id, String label, GradoopId sourceId, GradoopId targetId, Properties properties) {
        this(id, label, sourceId, targetId, properties, new GradoopIdSet());
    }

    public QueryEdge(GradoopId id, String label, GradoopId sourceId, GradoopId targetId, Properties properties, String variable) {
        this(id, label, sourceId, targetId, properties, variable, new GradoopIdSet());
    }

    public QueryEdge(GradoopId id, String label, GradoopId sourceId, GradoopId targetId, Properties properties, String variable, GradoopIdSet graphIds) {
        this(id, label, sourceId, targetId, properties, graphIds);
        variables.add(variable);
    }

    public String getVariable() {
        return this.variables.get(0);
    }

    QueryEdge() {
        selfPredicates = new ArrayList<>();
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    public void addPredicate(Predicate predicate) {
        /*if (selfPredicates == null) {
            selfPredicates = new ArrayList<>();
        }*/
        selfPredicates.add(predicate);
    }

    @Override
    public boolean hasPredicateSet() {
        return selfPredicates.size() > 0;
    }

    @Override
    public Collection<Predicate> getPredicates() {
        return this.selfPredicates;
    }

    public <E extends Element> boolean validatePredicate(E e) {
        boolean result = true;
        for (Predicate p : this.selfPredicates) { // only comparisons we have here and with values
            if (result) {
                if (p.getClass() == Comparison.class) {
                    Comparison comparison = (Comparison) p;
                    ComparableExpression[] list = comparison.getComparableExpressions();
                    if (list[0].getClass() == PropertySelector.class && list[1].getClass() == Literal.class) {
                        PropertySelector propertySelector = (PropertySelector) list[0];
                        var LeftPropertyValue = e.getPropertyValue(propertySelector.getPropertyName());
                        if (LeftPropertyValue != null) { // check here if right
                            Literal literal = (Literal) list[1];
                            PropertyValue rightPropertyValue = PropertyValue.create(literal.getValue());
                            result = evaluate(comparison, LeftPropertyValue, rightPropertyValue);
                        } else {
                            return false; // found null instead of value to compare with
                        }
                    }
                }
            } else {// last round was false
                return false;
            }
        }
        return true;
    }

    private boolean evaluate(Comparison comparison, PropertyValue leftPropertyValue, PropertyValue rightPropertyValue) {
        switch (comparison.getComparator()) {
            case EQ:
                return leftPropertyValue.compareTo(rightPropertyValue) == 0;
            case NEQ:
                return leftPropertyValue.compareTo(rightPropertyValue) != 0;
            case GT:
                return leftPropertyValue.compareTo(rightPropertyValue) > 0;
            case LT:
                return leftPropertyValue.compareTo(rightPropertyValue) < 0;
            case GTE:
                return leftPropertyValue.compareTo(rightPropertyValue) >= 0;
            case LTE:
                return leftPropertyValue.compareTo(rightPropertyValue) <= 0;
            default:
                throw new IllegalStateException("Unexpected value: " + comparison.getComparator());
        }
    }
}
