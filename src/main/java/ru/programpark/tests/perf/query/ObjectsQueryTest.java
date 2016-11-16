package ru.programpark.tests.perf.query;

import com.googlecode.cqengine.ConcurrentIndexedCollection;
import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.index.unique.UniqueIndex;
import com.googlecode.cqengine.query.QueryFactory;
import com.googlecode.cqengine.query.simple.Equal;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static ru.programpark.tests.perf.query.Fields.*;

/**
 * Created by kozyr on 26.01.2015.
 */
@State(Scope.Benchmark)
@Warmup(iterations = 2, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class ObjectsQueryTest {

    @Param({
            "10000", "100000", "250000", "500000", "1000000"
    })
    private int maxObjects;

    @Param({
            "true", "false"
    })
    private boolean needIndex;

    private IndexedCollection<ValueObject<Fields.VObject>> objects;
    private Long[] objIds;
    private Map<Long, ValueObject<Fields.VObject>> hash;

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
    }

    private void addIndices() {
        if (needIndex) {
            objects.addIndex(UniqueIndex.onAttribute(getAttr(Fields.VObject.id)));
        }
    }

    private int maxCnc() {
        return (maxObjects * 10) / 100;
    }

    @Benchmark
    public void randomBaseline(Blackhole bh) {
        bh.consume(objIds[Sequence.random(maxObjects)]);
    }

    @Benchmark
    public void createIdQuery(Blackhole bh) {
        Equal equal = QueryFactory.equal(getAttr(Fields.VObject.id), objIds[Sequence.random(maxObjects)]);
        bh.consume(equal);
    }

    @Benchmark
    public void querySizeById(Blackhole bh) {
        Equal equal = QueryFactory.equal(getAttr(Fields.VObject.id), objIds[Sequence.random(maxObjects)]);
        bh.consume(objects.retrieve(equal).size());
    }

    @Benchmark
    public void queryById(Blackhole bh) {
        Equal equal = QueryFactory.equal(getAttr(Fields.VObject.id), objIds[Sequence.random(maxObjects)]);
        bh.consume(objects.retrieve(equal).iterator().next());
    }

    @Benchmark
    public void queryHashById(Blackhole bh) {
        bh.consume(hash.get(Sequence.random(maxObjects)));
    }
}
