package io.deephaven.csvbench;

public class TableTextAndData {
    private final String text;
    private final Object[] columns;

    public TableTextAndData(String text, Object[] columns) {
        this.text = text;
        this.columns = columns;
    }

    public String text() {
        return text;
    }

    public Object[] columns() {
        return columns;
    }
}
