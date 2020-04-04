package streaming.model.grouping;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AggregationMapping implements Serializable {

    public HashMap<String, AggregationFunction> aggregationMap;

    public AggregationMapping(){
        aggregationMap = new HashMap<>();
    }

    public void addAggregation(String key, AggregationFunction accumulator){
        AggregationFunction aF = new AggregationFunction("0"){
            @Override
            public String apply(String pV1, String pV2) {
                return String.valueOf(Double.parseDouble(pV1) + Double.parseDouble(pV2));
            }
        };
        aggregationMap.put(key, aF);
    }

    public AggregationFunction get(String key){
        return aggregationMap.get(key);
    }

    public Set<Map.Entry<String, AggregationFunction>> entrySet(){
        return aggregationMap.entrySet();
    }

    public boolean contains(String key) {
        return aggregationMap.containsKey(key);
    }
}
