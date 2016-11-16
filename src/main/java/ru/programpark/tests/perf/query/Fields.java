package ru.programpark.tests.perf.query;

import com.googlecode.cqengine.attribute.Attribute;
import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.index.compound.support.CompoundAttribute;
import com.googlecode.cqengine.query.option.QueryOptions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * Created by kozyr on 14.11.2016.
 */
public class Fields {


    private static final Map<Enum, Attribute> attributes = new HashMap<>();

    static {
        createLongAttr(ObjectInContext.id);
        createLongAttr(ObjectInContext.cncId);
        createLongAttr(ObjectInContext.objId);
        createLongAttr(ObjectInContext.ctxId);
        attributes.put(ObjectInContext.compound, new CompoundAttribute<>(
                Arrays.asList(
                        getAttr(ObjectInContext.cncId),
                        getAttr(ObjectInContext.ctxId)
                )
        ));
        createLongAttr(VObject.id);
        createLongAttr(VObject.cncId);
        createLongAttr(VObject.lvalue);
        createStringAttr(VObject.svalue);
        createLongAttr(Context.id);
        createIdentityAttr(Identity.id, Long.class);
        createStringAttr(Context.name);
    }

    static long nextKey() {
        return Sequence.next();
    }

    static String[] generate(String prefix, int max) {
        return IntStream.iterate(0, (i) -> 1 + i).
                limit(max).mapToObj((j) -> prefix + j).toArray(String[]::new);
    }

    static Long[] generate(int max) {
        return generate(max, max, false);
    }

    static Long[] generate(int max, int maxRandom) {
        return generate(max, maxRandom, true);
    }

    private static Long[] generate(int max, int maxRandom, boolean random) {
        return IntStream.iterate(0, (i) -> random ? Sequence.random(maxRandom) : i + 1).
                limit(max).mapToObj((j) -> new Long(j)).toArray(Long[]::new);
    }

    static <T> T random(T[] values) {
        return values[Sequence.random(values.length)];
    }

    public static Attribute getAttr(Enum key) {
        return attributes.get(key);
    }

    private static void createLongAttr(Enum e) {
        SimpleAttribute<ValueObject, Long> simpleAttribute = new SimpleAttribute<ValueObject, Long>(ValueObject.class, Long.class, e.name()) {
            @Override
            public Long getValue(ValueObject o, QueryOptions queryOptions) {
                return (Long) o.getValue(e);
            }
        };
        attributes.put(e, simpleAttribute);
    }

    private static <T> void createIdentityAttr(Enum e, Class<T> c) {
        SimpleAttribute<T, T> simpleAttribute = new SimpleAttribute<T, T>(c, c, e.name()) {
            @Override
            public T getValue(T o, QueryOptions queryOptions) {
                return o;
            }
        };
        attributes.put(e, simpleAttribute);
    }

    private static void createStringAttr(Enum e) {
        SimpleAttribute<ValueObject, String> simpleAttribute = new SimpleAttribute<ValueObject, String>(ValueObject.class, String.class, e.name()) {
            @Override
            public String getValue(ValueObject o, QueryOptions queryOptions) {
                return (String) o.getValue(e);
            }
        };
        attributes.put(e, simpleAttribute);
    }

    enum Context {
        id, parent, name;

        public static ValueObject<Context> newObject(ValueObject<Context> parent, String name) {
            ValueObject<Context> res = new ValueObject(3);
            res.setValue(Context.id, nextKey());
            res.setValue(Context.name, name);
            if (parent != null) {
                res.setValue(Context.parent, parent.getId());
            }

            return res;
        }
    }

    enum ObjectInContext {
        id, objId, cncId, ctxId, compound;

        public static ValueObject<ObjectInContext> newObject(ValueObject<VObject> obj, ValueObject<Context> ctx) {
            ValueObject<ObjectInContext> res = new ValueObject(4);
            res.setValue(id, nextKey());
            res.setValue(cncId, obj.getValue(VObject.cncId));
            res.setValue(objId, obj.getId());
            res.setValue(ctxId, ctx.getId());
            return res;
        }
    }

    enum VObject {
        id, cncId, lvalue, svalue;

        public static ValueObject<VObject> newObject(int maxCnc) {
            ValueObject obj = new ValueObject<VObject>(4);
            obj.setValue(id, nextKey());
            obj.setValue(cncId, Sequence.random(maxCnc));
            return obj;
        }
    }

    enum Identity {id}
}
