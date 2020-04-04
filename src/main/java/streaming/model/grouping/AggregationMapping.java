package streaming.model.grouping;

import java.io.IOException;
import java.io.ObjectStreamException;
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
        AggregationFunction aF = accumulator;
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


    private void writeObject(java.io.ObjectOutputStream out)
            throws IOException {
        out.writeInt(aggregationMap.size());
        for(Map.Entry<String, AggregationFunction> entry : aggregationMap.entrySet()){
            out.writeObject(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        this.aggregationMap = new HashMap<>();
        int size = in.readInt();
        for(int i=0;i<size;i++) {
            String key = (String) in.readObject();
            System.out.println(key);
            AggregationFunction aggFun = (AggregationFunction) in.readObject();
            System.out.println(aggFun);
            aggregationMap.put(key, aggFun);
        }

    }
    private void readObjectNoData()
            throws ObjectStreamException {

    }
}
