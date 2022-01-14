package io.deephaven;

import gnu.trove.list.array.*;
import io.deephaven.csv.reading.CsvReader;
import io.deephaven.csv.sinks.Sink;
import io.deephaven.csv.sinks.SinkFactory;
import io.deephaven.csv.sinks.Source;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.csv.util.Renderer;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

public class TestInts {
    @Test
    public void deephaven() throws CsvReaderException {
        final Random rng = new Random(12345);
        final TextAndNubbins tns = buildTable(rng, 1000, 1);
        final InputStream bais = new ByteArrayInputStream(tns.text.getBytes(StandardCharsets.UTF_8));

        final CsvReader reader = new CsvReader();
        final SinkFactory sf = SinkFactory.of(
                null, null,
                null, null,
                MyIntSink::new, null,
                null, null,
                null, null,
                null, null,
                null, null,
                null, null,
                null, null,
                null, null,
                null);
        reader.read(bais, sf);
    }

    public static TextAndNubbins buildTable(final Random rng, final int numRows, final int numCols) {
        final TextAndValues[] tvs = new TextAndValues[numCols];
        for (int ii = 0; ii < numCols; ++ii) {
            tvs[ii] = makeIntegers(rng, numRows);
        }

        final StringBuilder sb = new StringBuilder();
        // Write a line of headers like Column1,Column2,...,ColumnN
        Renderer.renderList(sb, IntStream.range(0, numCols)::iterator, ",", i -> "Column" + (i + 1));
        sb.append('\n');
        for (int jj = 0; jj < numRows; ++jj) {
            final int finalJJ = jj;
            // Write a line of data like 12,-54321,...,17
            Renderer.renderList(sb, IntStream.range(0, numCols)::iterator, ",", i -> tvs[i].text[finalJJ]);
            sb.append('\n');
        }

        final String text = sb.toString();
        final Object[] nubbins = new Object[numCols];
        for (int ii = 0; ii < numCols; ++ii) {
            nubbins[ii] = tvs[ii].data;
        }

        return new TextAndNubbins(text, nubbins);
    }

    private static TextAndValues makeIntegers(Random rng, final int numRows) {
        final String[] text = new String[numRows];
        final int[] data = new int[numRows];
        for (int ii = 0; ii < numRows; ++ii) {
            final int nextValue = rng.nextInt();
            data[ii] = nextValue;
            text[ii] = Integer.toString(nextValue);
        }
        return new TextAndValues(text, data);
    }

    private static class TextAndNubbins {
        private final String text;
        private final Object[] nubbins;

        public TextAndNubbins(String text, Object[] nubbins) {
            this.text = text;
            this.nubbins = nubbins;
        }
    }

    private static class TextAndValues {
        private final String[] text;
        private final int[] data;

        public TextAndValues(String[] text, int[] data) {
            this.text = text;
            this.data = data;
        }
    }

    public interface ColumnProvider<TARRAY> {
        Column<TARRAY> toColumn(final String columnName);
    }

    private static abstract class MySinkBase<TCOLLECTION, TARRAY> implements Sink<TARRAY>, ColumnProvider<TARRAY> {
        protected final TCOLLECTION collection;
        protected int collectionSize;
        protected final FillOperation<TCOLLECTION> fillOperation;
        protected final SetOperation<TCOLLECTION, TARRAY> setOperation;
        protected final AddOperation<TCOLLECTION, TARRAY> addOperation;
        protected final BiFunction<TCOLLECTION, String, Column<TARRAY>> toColumnOperation;

        protected MySinkBase(TCOLLECTION collection, FillOperation<TCOLLECTION> fillOperation,
                             SetOperation<TCOLLECTION, TARRAY> setOperation, AddOperation<TCOLLECTION, TARRAY> addOperation,
                             BiFunction<TCOLLECTION, String, Column<TARRAY>> toColumnOperation) {
            this.collection = collection;
            this.fillOperation = fillOperation;
            this.setOperation = setOperation;
            this.addOperation = addOperation;
            this.toColumnOperation = toColumnOperation;
        }

        @Override
        public final void write(final TARRAY src, final boolean[] isNull, final long destBegin,
                                final long destEnd, boolean appending) {
            if (destBegin == destEnd) {
                return;
            }
            final int size = Math.toIntExact(destEnd - destBegin);
            final int destBeginAsInt = Math.toIntExact(destBegin);
            final int destEndAsInt = Math.toIntExact(destEnd);
            nullFlagsToValues(isNull, src, size);

            if (!appending) {
                // Replacing.
                setOperation.apply(collection, destBeginAsInt, src, 0, size);
                return;
            }

            // Appending. First, if the new area starts beyond the end of the destination, pad the destination.
            if (collectionSize < destBegin) {
                fillOperation.apply(collection, collectionSize, destBeginAsInt);
                collectionSize = destBeginAsInt;
            }
            // Then do the append.
            addOperation.apply(collection, src, 0, size);
            collectionSize = destEndAsInt;
        }

        public final Column<TARRAY> toColumn(final String columnName) {
            return toColumnOperation.apply(collection, columnName);
        }

        /**
         * Meant to be paired with e.g. TDoubleArrayList.fill(int fromIndex, int toIndex, 0.0)
         */
        protected interface FillOperation<TCOLLECTION> {
            void apply(TCOLLECTION coll, int fromIndex, int toIndex);
        }

        /**
         * Meant to be paired with e.g. TDoubleArrayList.set(int offset, double[] values, int valOffset, int length)
         */
        protected interface SetOperation<TCOLLECTION, TARRAY> {
            void apply(TCOLLECTION coll, int offset, TARRAY values, int vallOffset, int length);
        }

        /**
         * Meant to be paired with e.g. TDoubleArrayList.add(double[] values, int offset, int length)
         */
        protected interface AddOperation<TCOLLECTION, TARRAY> {
            void apply(TCOLLECTION coll, TARRAY values, int offset, int length);
        }
    }

    private static abstract class MySourceAndSinkBase<TCOLLECTION, TARRAY> extends MySinkBase<TCOLLECTION, TARRAY>
            implements io.deephaven.csv.sinks.Source<TARRAY>, Sink<TARRAY> {
        private final ToArrayOperation<TCOLLECTION, TARRAY> toArrayOperation;

        protected MySourceAndSinkBase(TCOLLECTION collection, FillOperation<TCOLLECTION> fillOperation,
                                      SetOperation<TCOLLECTION, TARRAY> setOperation, AddOperation<TCOLLECTION, TARRAY> addOperation,
                                      BiFunction<TCOLLECTION, String, Column<TARRAY>> toColumnOperation,
                                      ToArrayOperation<TCOLLECTION, TARRAY> toArrayOperation) {
            super(collection, fillOperation, setOperation, addOperation, toColumnOperation);
            this.toArrayOperation = toArrayOperation;
        }

        @Override
        public void read(TARRAY dest, boolean[] isNull, long srcBegin, long srcEnd) {
            if (srcBegin == srcEnd) {
                return;
            }
            final int size = Math.toIntExact(srcEnd - srcBegin);
            toArrayOperation.apply(collection, dest, Math.toIntExact(srcBegin), 0, size);
        }

        /**
         * Meant to be paired with e.g. TDoubleArrayList.add(double[] dest, int source_pos, int dest_pos, int length)
         */
        private interface ToArrayOperation<TCOLLECTION, TARRAY> {
            void apply(TCOLLECTION coll, TARRAY dest, int source_pos_, int dest_pos, int length);
        }
    }

    private static class MyByteSinkBase extends MySourceAndSinkBase<TByteArrayList, byte[]> {
        protected final byte nullSentinel;

        public MyByteSinkBase(final byte nullSentinel, final Class<?> reinterpretedType) {
            super(new TByteArrayList(),
                    (dest, from, to) -> dest.fill(from, to, (byte) 0),
                    TByteArrayList::set,
                    TByteArrayList::add,
                    (dest, name) -> Column.ofArray(name, dest.toArray()).reinterpret(reinterpretedType),
                    TByteArrayList::toArray);
            this.nullSentinel = nullSentinel;
        }
    }

    private static final class MyByteSink extends MyByteSinkBase {
        public MyByteSink() {
            super(Sentinels.NULL_BYTE, byte.class);
        }
    }

    private static final class MyShortSink extends MySourceAndSinkBase<TShortArrayList, short[]> {
        public MyShortSink() {
            super(new TShortArrayList(),
                    (dest, from, to) -> dest.fill(from, to, (short) 0),
                    TShortArrayList::set,
                    TShortArrayList::add,
                    (dest, name) -> Column.ofArray(name, dest.toArray()),
                    TShortArrayList::toArray);
        }
    }

    private static final class MyIntSink extends MySourceAndSinkBase<TIntArrayList, int[]> {
        public MyIntSink() {
            super(new TIntArrayList(),
                    (dest, from, to) -> dest.fill(from, to, 0),
                    TIntArrayList::set,
                    TIntArrayList::add,
                    (dest, name) -> Column.ofArray(name, dest.toArray()),
                    TIntArrayList::toArray);
        }
    }

    private static class MyLongSinkBase extends MySourceAndSinkBase<TLongArrayList, long[]> {
        public MyLongSinkBase(final Class<?> reinterpretedType) {
            super(new TLongArrayList(),
                    (dest, from, to) -> dest.fill(from, to, 0L),
                    TLongArrayList::set,
                    TLongArrayList::add,
                    (dest, name) -> Column.ofArray(name, dest.toArray()).reinterpret(reinterpretedType),
                    TLongArrayList::toArray);
        }
    }

    private static final class MyLongSink extends MyLongSinkBase {
        public MyLongSink() {
            super(Sentinels.NULL_LONG, long.class);
        }
    }

    private static final class MyFloatSink extends MySinkBase<TFloatArrayList, float[]> {
        public MyFloatSink() {
            super(new TFloatArrayList(),
                    (dest, from, to) -> dest.fill(from, to, 0),
                    TFloatArrayList::set,
                    TFloatArrayList::add,
                    (dest, name) -> Column.ofArray(name, dest.toArray()));
        }
    }

    private static final class MyDoubleSink extends MySinkBase<TDoubleArrayList, double[]> {
        public MyDoubleSink() {
            super(new TDoubleArrayList(),
                    (dest, from, to) -> dest.fill(from, to, 0),
                    TDoubleArrayList::set,
                    TDoubleArrayList::add,
                    (dest, name) -> Column.ofArray(name, dest.toArray()));
        }
    }

    private static final class MyBooleanAsByteSink extends MyByteSinkBase {
        public MyBooleanAsByteSink() {
            super(Sentinels.NULL_BOOLEAN_AS_BYTE, boolean.class);
        }
    }

    private static final class MyCharSink extends MySinkBase<TCharArrayList, char[]> {
        public MyCharSink() {
            super(new TCharArrayList(),
                    (coll, from, to) -> coll.fill(from, to, (char) 0),
                    TCharArrayList::set,
                    TCharArrayList::add,
                    (dest, name) -> Column.ofArray(name, dest.toArray()));
        }
    }

    private static final class MyStringSink extends MySinkBase<ArrayList<String>, String[]> {
        public MyStringSink() {
            super(new ArrayList<>(),
                    MyStringSink::fill,
                    MyStringSink::set,
                    MyStringSink::add,
                    (dest, name) -> Column.ofArray(name, dest.toArray(new String[0])));
        }

        private static void fill(final ArrayList<String> dest, final int from, final int to) {
            for (int current = from; current != to; ++current) {
                if (current < dest.size()) {
                    dest.set(current, null);
                } else {
                    dest.add(null);
                }
            }
        }

        private static void set(final ArrayList<String> dest, final int destOffset, final String[] src,
                                final int srcOffset, final int size) {
            for (int ii = 0; ii < size; ++ii) {
                dest.set(destOffset + ii, src[srcOffset + ii]);
            }
        }

        private static void add(final ArrayList<String> dest, final String[] src, final int srcOffset,
                                final int size) {
            for (int ii = 0; ii < size; ++ii) {
                dest.add(src[srcOffset + ii]);
            }
        }
    }

    private static final class MyDateTimeAsLongSink extends MyLongSinkBase {
        public MyDateTimeAsLongSink() {
            super(null, Instant.class);
        }
    }

    private static final class MyTimestampAsLongSink extends MyLongSinkBase {
        public MyTimestampAsLongSink() {
            super(null, Instant.class);
        }
    }

    private static SinkFactory makeMySinkFactory() {
        return SinkFactory.of(
                MyByteSink::new, null,
                MyShortSink::new, null,
                MyIntSink::new, null,
                MyLongSink::new, null,
                MyFloatSink::new, null,
                MyDoubleSink::new, null,
                MyBooleanAsByteSink::new,
                MyCharSink::new, null,
                MyStringSink::new, null,
                MyDateTimeAsLongSink::new, null,
                MyTimestampAsLongSink::new, null);
    }
}

