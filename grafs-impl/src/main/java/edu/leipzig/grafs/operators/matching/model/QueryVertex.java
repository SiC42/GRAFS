package edu.leipzig.grafs.operators.matching.model;

import edu.leipzig.grafs.model.Element;
import edu.leipzig.grafs.model.Vertex;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.s1ck.gdl.model.comparables.ComparableExpression;
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.comparables.PropertySelector;
import org.s1ck.gdl.model.predicates.Predicate;
import org.s1ck.gdl.model.predicates.expressions.Comparison;

public class QueryVertex extends Vertex implements HasPredicate {
    Collection<Predicate> selfPredicates;
    private ArrayList<String> variables = new ArrayList<>();

    public QueryVertex() {
        selfPredicates = new ArrayList<>();
    }


    public QueryVertex(GradoopId id, String label, Properties properties,  GradoopIdSet gradoopIds) {
        super(id, label, properties, gradoopIds);
        selfPredicates = new ArrayList<>();
        variables = new ArrayList<>();
    }

    public QueryVertex(GradoopId id, String label, Properties properties) {
        this(id, label, properties, new GradoopIdSet());
    }

    public QueryVertex(GradoopId id, String label, Properties properties, String variable) {
        this(id, label, properties, new GradoopIdSet(), variable);
    }

    public QueryVertex(GradoopId id, String label, Properties properties, GradoopIdSet gradoopIds, String variable) {
        this(id, label, properties, gradoopIds);

        variables.add(variable);
    }

    public void addVariable(String variable) {
        this.variables.add(variable);
    }

    public String getVariable() {
        return this.variables.get(0);
    }

    public ArrayList<String> getVariables(){
        return this.variables;
    }

    public boolean hasVariable(String variable) {
        boolean result = false;
        for (String var : this.variables) {
            if (var.equals(variable)) {
                result = true;
                break;
            }
        }
        return result;
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
                        PropertyValue vertexPropertyValue = e.getProperties().get(propertySelector.getPropertyName());
                        if (vertexPropertyValue != null) { // check here if right
                            Literal literal = (Literal) list[1];
                            PropertyValue literalPropertyValue = PropertyValue.create(literal.getValue());
                            switch (comparison.getComparator()) {
                                case NEQ:
                                    result = vertexPropertyValue.compareTo(literalPropertyValue) != 0;
                                    break;
                                case GT:
                                    result = vertexPropertyValue.compareTo(literalPropertyValue) > 0;
                                    break;
                                case LT:
                                    result = vertexPropertyValue.compareTo(literalPropertyValue) < 0;
                                    break;
                                case GTE:
                                    result = vertexPropertyValue.compareTo(literalPropertyValue) >= 0;
                                    break;
                                case LTE:
                                    result = vertexPropertyValue.compareTo(literalPropertyValue) <= 0;
                                    break;
                            }
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

    @Override
    public String toString() {
        return String.format("(%s, variables=%s,selfPredicates=%s)", super.toString(),variables.toString(), selfPredicates.toString());
    }

    /**
     * Returns <tt>true</tt>> if the given object is an <tt>Element</tt> and the IDs match with this
     * element.
     *
     * @param o object to be tested for equality
     * @return <tt>true</tt> if the other object is an <tt>Element</tt> and the IDs match.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        QueryVertex that = (QueryVertex) o;

        return Objects.equals(getId(), that.getId()) && Objects.equals(variables, that.variables);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}