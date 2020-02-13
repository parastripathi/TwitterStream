package entity;

import java.util.Vector;

public class RedisStorage {
    Vector<Vector<String>> tupleBucket;

    public Vector<Vector<String>> getTupleBucket() {
        return tupleBucket;
    }

    public void setTupleBucket(Vector<Vector<String>> tupleBucket) {
        this.tupleBucket = tupleBucket;
    }
}
