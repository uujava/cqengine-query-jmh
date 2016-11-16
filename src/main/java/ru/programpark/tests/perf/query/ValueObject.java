package ru.programpark.tests.perf.query;


public class ValueObject<T extends Enum> {

    private Object[] values;

    public ValueObject(int size) {
        this.values = new Object[size];
    }

    public Long getId() {
        return (Long) values[0];
    }

    public Object getValue(T name) {
        return values[name.ordinal()];
    }

    public void setValue(T name, Object value) {
        values[name.ordinal()] = value;
    }

}
