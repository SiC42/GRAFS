package edu.leipzig.grafs.operators.matching.model;

import java.util.Collection;
import org.s1ck.gdl.model.predicates.Predicate;

public interface HasPredicate {

  void addPredicate(Predicate predicate);

  void setProperty(String key, Object value);

  boolean hasPredicateSet();

  Collection<Predicate> getPredicates();
}
