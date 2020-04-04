package streaming.model.grouping;

import java.io.Serializable;

public abstract class AggregationFunction implements Serializable {

    private String identity;

    public AggregationFunction(String identity) {
        this.identity = identity;
    }

    public String getIdentity() {
        return identity;
    }

    public abstract String apply(String pV1, String pV2);
}
