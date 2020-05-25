package streaming.operators.grouping.functions;

import java.io.Serializable;
import java.util.function.BiFunction;

public interface SerializableBiFunction<S, T, R> extends BiFunction<S, T, R>, Serializable {

}