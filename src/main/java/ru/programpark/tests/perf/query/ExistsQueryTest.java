package ru.programpark.tests.perf.query;

import com.googlecode.cqengine.ConcurrentIndexedCollection;
import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.index.unique.UniqueIndex;
import com.googlecode.cqengine.query.QueryFactory;
import com.googlecode.cqengine.query.simple.Equal;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static ru.programpark.tests.perf.query.Fields.generate;
import static ru.programpark.tests.perf.query.Fields.random;
import static ru.programpark.tests.perf.query.Fields.getAttr;

/**
 * Created by kozyr on 26.01.2015.
 */
@State(Scope.Benchmark)
@Warmup(iterations = 2, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(2)
public class ExistsQueryTest {

    @Param({
            "100000", "250000", "500000"
    })
    private int maxObjects;

    @Param({
            "1",
            "5",
            "10",
            "20"
    })
    private int ratio;

    @Param({
            "10000"
    })
    private int totalCtx;

    @Param({
            "1",
            "2",
            "3"
    })
    private int maxCtx;

    private IndexedCollection<ValueObject<Fields.VObject>> objects;
    private IndexedCollection<ValueObject<Fields.Context>> contexts;
    private IndexedCollection<ValueObject<Fields.ObjectInContext>> oics;
    private Long[] objIds;

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        objects = new ConcurrentIndexedCollection<>();
        contexts = new ConcurrentIndexedCollection<>();
        oics = new ConcurrentIndexedCollection<>();
        addIndices();
        // generate contexts
        ValueObject<Fields.Context>[] ctxes = Stream.of(generate("ctx", totalCtx))
                .map((name) -> Fields.Context.newObject(null, name))
                .peek(c -> contexts.add(c)).toArray(size -> new ValueObject[size]);
        String[] svalues = generate("s", maxCnc() / 100);
        Long[] lvalues = generate(maxCnc() / 10);
        objIds = new Long[maxObjects];
        // generate objects
        for (int i = 0; i < maxObjects; i++) {
            ValueObject<Fields.VObject> object = Fields.VObject.newObject(maxCnc());
            object.setValue(Fields.VObject.lvalue, random(lvalues));
            object.setValue(Fields.VObject.svalue, random(svalues));
            objects.add(object);
            objIds[i] = object.getId();
        }
       // put object into contexts
        objects.forEach((v) -> {
                    for (int i = 0; i < maxCtx; i++) {
                        oics.add(Fields.ObjectInContext.newObject(v, random(ctxes)));
                    }
                }
        );
    }

    private void addIndices() {
        objects.addIndex(UniqueIndex.onAttribute(getAttr(Fields.VObject.id)));
        oics.addIndex(UniqueIndex.onAttribute(getAttr(Fields.ObjectInContext.objId)));
        contexts.addIndex(UniqueIndex.onAttribute(getAttr(Fields.Context.name)));
    }

    private int maxCnc() {
        return (maxObjects * ratio) / 100;
    }

    @Benchmark
    public void queryByIdBaseline(Blackhole bh) {
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

}
