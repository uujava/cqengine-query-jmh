package ru.programpark.tests.perf.query;

import com.googlecode.cqengine.ConcurrentIndexedCollection;
import com.googlecode.cqengine.IndexedCollection;
import com.googlecode.cqengine.index.hash.HashIndex;
import com.googlecode.cqengine.index.unique.UniqueIndex;
import com.googlecode.cqengine.query.Query;
import com.googlecode.cqengine.query.option.QueryOptions;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

import static com.googlecode.cqengine.query.QueryFactory.*;
import static ru.programpark.tests.perf.query.Fields.*;
import static ru.programpark.tests.perf.query.Sequence.*;
import static ru.programpark.tests.perf.query.Sequence.random;

/**
 * Created by kozyr on 26.01.2015.
 */
@State(Scope.Benchmark)
@Warmup(iterations = 2, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(2)
public class QueryExistsInTest {
    // Object count
    @Param({
            "100000", "500000"
    })
    private int totalObjects;
    // Object objects/concept ratio in percent
    @Param({
            "50",
            "1000",
    })
    private int totalConcepts;

    @Param({
            "10000"
    })
    private int totalContexts;

    @Param({
            "3"
    })
    private int maxCtx;

    private IndexedCollection<ValueObject<VObject>> objects;
    private IndexedCollection<ValueObject<Context>> contexts;
    private IndexedCollection<ValueObject<ObjectInContext>> oics;
    private Long[] objIds;
    private Long[] ctxIds;
    private String[] svalues;
    private Long[] lvalues;
    private String[] ctxNames;
    private QueryOptions options;

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        objects = new ConcurrentIndexedCollection<>();
        contexts = new ConcurrentIndexedCollection<>();
        oics = new ConcurrentIndexedCollection<>();
        addIndices();
        // generate contexts
        ctxNames = generate("ctx", totalContexts);
        ValueObject<Context>[] ctxes = (ValueObject<Context>[]) new ValueObject[ctxNames.length];
        ctxIds = new Long[ctxNames.length];
        for (int i = 0; i < ctxNames.length; i++) {
            String ctxName = ctxNames[i];
            ValueObject<Context> ctx = Context.newObject(null, ctxName);
            contexts.add(ctx);
            ctxes[i] = ctx;
            ctxIds[i] = ctx.getId();
        }
        svalues = generate("s", totalConcepts * 10);
        lvalues = generate(totalConcepts * 100);
        objIds = new Long[totalObjects];
        // generate objects
        for (int i = 0; i < totalObjects; i++) {
            ValueObject<VObject> object = VObject.newObject(totalConcepts);
            object.setValue(VObject.lvalue, randomObject(lvalues));
            object.setValue(VObject.svalue, randomObject(svalues));
            objects.add(object);
            objIds[i] = object.getId();
        }
        // put object into contexts
        objects.forEach((v) -> {
                    for (int i = 0; i < maxCtx; i++) {
                        oics.add(ObjectInContext.newObject(v, randomObject(ctxes)));
                    }
                }
        );
        options = new QueryOptions();
    }

    private void addIndices() {
        objects.addIndex(UniqueIndex.onAttribute(getAttr(VObject.id)));
        oics.addIndex(HashIndex.onAttribute(getAttr(ObjectInContext.ctxId)));
        oics.addIndex(HashIndex.onAttribute(getAttr(ObjectInContext.cncId)));
        oics.addIndex(HashIndex.onAttribute(getAttr(ObjectInContext.objId)));
        contexts.addIndex(UniqueIndex.onAttribute(getAttr(Context.name)));
    }

    @Benchmark
    public void queryFirstByOic(Blackhole bh) {
        Query oicQuery = and(
                equal(getAttr(ObjectInContext.ctxId), randomObject(ctxIds)),
                equal(getAttr(ObjectInContext.cncId), (long) random(totalConcepts))
        );
        Query existsInOic = existsIn(oics, getAttr(VObject.id), getAttr(ObjectInContext.objId), oicQuery);
        bh.consume(asStream(objects, existsInOic, options).findFirst());
    }

    @Benchmark
    public void queryFirstByOicAndLvalue(Blackhole bh) {
        Query oicQuery = and(
                equal(getAttr(ObjectInContext.ctxId), randomObject(ctxIds)),
                equal(getAttr(ObjectInContext.cncId), (long) random(totalConcepts))
        );
        Query existsInOic = existsIn(oics, getAttr(VObject.id), getAttr(ObjectInContext.objId), oicQuery);
        Query lvalueQuery = equal(getAttr(VObject.lvalue), randomObject(lvalues));
        bh.consume(asStream(objects, and(lvalueQuery, existsInOic), options));
    }

    @Benchmark
    public void queryFirstByOicAndSvalue(Blackhole bh) {
        Query oicQuery = and(
                equal(getAttr(ObjectInContext.ctxId), randomObject(ctxIds)),
                equal(getAttr(ObjectInContext.cncId), (long) random(totalConcepts))
        );
        Query existsInOic = existsIn(oics, getAttr(VObject.id), getAttr(ObjectInContext.objId), oicQuery);
        Query svalueQuery = equal(getAttr(VObject.svalue), randomObject(svalues));
        bh.consume(asStream(objects, and(svalueQuery, existsInOic), options));
    }

}
