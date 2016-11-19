package ru.programpark.tests.perf.query;

import com.googlecode.cqengine.ConcurrentIndexedCollection;
import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.index.hash.HashIndex;
import com.googlecode.cqengine.index.unique.UniqueIndex;
import com.googlecode.cqengine.query.Query;
import com.googlecode.cqengine.query.QueryFactory;
import com.googlecode.cqengine.query.option.DeduplicationStrategy;
import com.googlecode.cqengine.query.option.QueryOptions;
import com.googlecode.cqengine.query.simple.Equal;
import com.googlecode.cqengine.resultset.stored.StoredSetBasedResultSet;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static ru.programpark.tests.perf.query.Fields.*;
import static ru.programpark.tests.perf.query.Sequence.*;

/**
 * Created by kozyr on 26.01.2015.
 */
@State(Scope.Benchmark)
@Warmup(iterations = 2, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class QueryValueObjectTest {

    @Param({
            "10000", "100000", "1000000"
    })
    private int maxObjects;

    @Param({
            "true", "false"
    })
    private boolean uniqueIndex;

    @Param({
            "50"
    })
    private int maxInIds;

    private IndexedCollection<ValueObject<Fields.VObject>> objects;
    private Long[] objIds;
    private Map<Long, ValueObject<Fields.VObject>> hash;
    private QueryOptions options;
    private QueryOptions deduplicate;

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        objects = new ConcurrentIndexedCollection<>();
        addIndices();
        // generate contexts
        String[] svalues = generate("s", maxCnc() / 100);
        Long[] lvalues = generate(maxCnc() / 10);
        objIds = new Long[maxObjects];
        // generate objects
        hash = new HashMap();
        for (int i = 0; i < maxObjects; i++) {
            ValueObject<Fields.VObject> object = Fields.VObject.newObject(maxCnc());
            object.setValue(Fields.VObject.lvalue, random(lvalues));
            object.setValue(Fields.VObject.svalue, random(svalues));
            objects.add(object);
            objIds[i] = object.getId();
            hash.put(object.getId(), object);
        }
        options = new QueryOptions();
        deduplicate = new QueryOptions();
        deduplicate.put(DeduplicationStrategy.class, DeduplicationStrategy.MATERIALIZE);
    }

    private void addIndices() {
        if (uniqueIndex) {
            objects.addIndex(UniqueIndex.onAttribute(getAttr(Fields.VObject.id)));
        } else {
            objects.addIndex(HashIndex.onAttribute(() -> new ConcurrentHashMap(maxObjects), () -> new StoredSetBasedResultSet<>(new HashSet(1)), getAttr(Fields.VObject.id)));
        }
    }

    private int maxCnc() {
        return (maxObjects * 10) / 100;
    }

    @Benchmark
    public void randomBaseline(Blackhole bh) {
        bh.consume(randomObject(objIds));
    }

    @Benchmark
    public void sizeById(Blackhole bh) {
        Equal equal = QueryFactory.equal(getAttr(Fields.VObject.id), randomObject(objIds));
        bh.consume(objects.retrieve(equal).size());
    }

    @Benchmark
    public void queryById(Blackhole bh) {
        Equal equal = QueryFactory.equal(getAttr(Fields.VObject.id), randomObject(objIds));
        Object next = objects.retrieve(equal).iterator().next();
        if(next != null) {
            bh.consume(next);
        }
    }

    @Benchmark
    public void queryByIdStream(Blackhole bh) {
        Equal equal = QueryFactory.equal(getAttr(Fields.VObject.id), randomObject(objIds));
        bh.consume(asStream(objects, equal, options).findFirst());
    }


    @Benchmark
    public void queryIn(Blackhole bh) {
        Query query = QueryFactory.in(getAttr(VObject.id), randomList(objIds,  maxInIds));
        Iterable<ValueObject<VObject>> iterator = objects.retrieve(query, options);
        for (ValueObject<VObject> object : iterator) {
            bh.consume(object);
        }
    }

    @Benchmark
    public void queryInDeduplicate(Blackhole bh) {
        Query query = QueryFactory.in(getAttr(Fields.VObject.id), randomList(objIds,  maxInIds));
        Iterable<ValueObject<VObject>> iterator = objects.retrieve(query, deduplicate);
        for (ValueObject<VObject> object : iterator) {
            bh.consume(object);
        }
    }

    @Benchmark
    public void queryInStream(Blackhole bh) {
        Query query = QueryFactory.in(getAttr(Fields.VObject.id), randomList(objIds,  maxInIds));
        asStream(objects, query, options).forEach(o -> bh.consume(o));
    }

    @Benchmark
    public void queryInStreamDeduplicate(Blackhole bh) {
        Query query = QueryFactory.in(getAttr(Fields.VObject.id), randomList(objIds,  maxInIds));
        asStream(objects, query, options).distinct().forEach(o -> bh.consume(o));
    }
}
