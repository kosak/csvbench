package io.deephaven.csvbench;

import gnu.trove.list.array.*;
import io.deephaven.csv.util.Renderer;
import org.assertj.core.api.Assertions;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

public class BenchmarkInts {
    private Random rng;
    private TableTextAndData tableTextAndData;
    private ByteArrayInputStream tableTextStream;
    private int[] expectedResult;
    private int[] actualResult;

    public void setup() {
        rng = new Random(12345);
        tableTextAndData = makeTable(rng, 1000, 1);
        tableTextStream = new ByteArrayInputStream(tableTextAndData.text().getBytes(StandardCharsets.UTF_8));
        expectedResult = (int[])tableTextAndData.columns()[0];
    }

    public void checkResult() {
        Assertions.assertThat(actualResult).isEqualTo(expectedResult);
    }

    public void teardown() {
    }

    public static TableTextAndData makeTable(final Random rng, final int numRows, final int numCols) {
        final List<ColumnTextAndData<int[]>> tvs = new ArrayList<>();
        for (int ii = 0; ii < numCols; ++ii) {
            tvs.add(makeIntegerColumn(rng, numRows));
        }
        return TableTextAndData.of(tvs);
    }

    private static ColumnTextAndData<int[]> makeIntegerColumn(Random rng, final int numRows) {
        final String[] text = new String[numRows];
        final int[] data = new int[numRows];
        for (int ii = 0; ii < numRows; ++ii) {
            final int nextValue = rng.nextInt();
            data[ii] = nextValue;
            text[ii] = Integer.toString(nextValue);
        }
        return new ColumnTextAndData<>(text, data);
    }

    public void deephaven() throws io.deephaven.csv.util.CsvReaderException {
        final io.deephaven.csv.reading.CsvReader reader = new io.deephaven.csv.reading.CsvReader();
        final io.deephaven.csv.sinks.SinkFactory sf = makeMySinkFactory();
        final io.deephaven.csv.reading.CsvReader.Result result = reader.read(tableTextStream, sf);

        ResultProvider<?> rp = (ResultProvider<?>) result.columns()[0];
        actualResult = (int[]) rp.toResult();
    }

    public void apacheCommons() throws IOException {
        final Random rng = new Random(12345);
        final TextAndNubbins tns = buildTable(rng, 1000, 1);

        final org.apache.commons.csv.CSVFormat format = org.apache.commons.csv.CSVFormat.DEFAULT
                .builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                .setRecordSeparator('\n')
                .build();

        final org.apache.commons.csv.CSVParser parser = new org.apache.commons.csv.CSVParser(new StringReader(tns.text), format);

        final TIntArrayList results = new TIntArrayList();
        for (org.apache.commons.csv.CSVRecord next : parser) {
            results.add(Integer.parseInt(next.get(0)));
        }
        final int[] typedData = results.toArray();
        Assertions.assertThat(typedData).isEqualTo(tns.nubbins[0]);
    }

    public void fastCsv() {
        final Random rng = new Random(12345);
        final TextAndNubbins tns = buildTable(rng, 1000, 1);

        final de.siegmar.fastcsv.reader.CloseableIterator<de.siegmar.fastcsv.reader.CsvRow> iterator = de.siegmar.fastcsv.reader.CsvReader.builder()
                .build(tns.text)
                .iterator();

        final TIntArrayList results = new TIntArrayList();
        // Skip header row
        if (iterator.hasNext()) {
            iterator.next();
        }
        while (iterator.hasNext()) {
            final de.siegmar.fastcsv.reader.CsvRow next = iterator.next();
            results.add(Integer.parseInt(next.getField(0)));
        }
        final int[] typedData = results.toArray();
        Assertions.assertThat(typedData).isEqualTo(tns.nubbins[0]);
    }

    public void jacksonCsv() throws IOException {
        final Random rng = new Random(12345);
        final TextAndNubbins tns = buildTable(rng, 1000, 1);

        final com.fasterxml.jackson.databind.MappingIterator<List<String>> iterator = new com.fasterxml.jackson.dataformat.csv.CsvMapper()
                .enable(com.fasterxml.jackson.dataformat.csv.CsvParser.Feature.WRAP_AS_ARRAY)
                .readerFor(List.class)
                .readValues(tns.text);

        final TIntArrayList results = new TIntArrayList();
        // Skip header row
        if (iterator.hasNext()) {
            iterator.next();
        }
        while (iterator.hasNext()) {
            final List<String> next = iterator.next();
            results.add(Integer.parseInt(next.get(0)));
        }
        final int[] typedData = results.toArray();
        Assertions.assertThat(typedData).isEqualTo(tns.nubbins[0]);
    }

    public void openCsv() throws IOException, com.opencsv.exceptions.CsvValidationException {
        final Random rng = new Random(12345);
        final TextAndNubbins tns = buildTable(rng, 1000, 1);

        final com.opencsv.CSVReader csvReader = new com.opencsv.CSVReader(new StringReader(tns.text));
        final TIntArrayList results = new TIntArrayList();
        if (csvReader.readNext() == null) {
            throw new RuntimeException("Expected header row");
        }
        while (true) {
            final String[] next = csvReader.readNext();
            if (next == null) {
                break;
            }
            results.add(Integer.parseInt(next[0]));
        }
        final int[] typedData = results.toArray();
        Assertions.assertThat(typedData).isEqualTo(tns.nubbins[0]);
    }

    public void simpleFlatMapper() throws IOException {
        final Random rng = new Random(12345);
        final TextAndNubbins tns = buildTable(rng, 1000, 1);

        Iterator<String[]> iterator = org.simpleflatmapper.lightningcsv.CsvParser.iterator(tns.text);
        // Skip header row
        if (iterator.hasNext()) {
            iterator.next();
        }

        final TIntArrayList results = new TIntArrayList();
        while (iterator.hasNext()) {
            final String[] next = iterator.next();
            results.add(Integer.parseInt(next[0]));
        }
        final int[] typedData = results.toArray();
        Assertions.assertThat(typedData).isEqualTo(tns.nubbins[0]);
    }

    public void superCsv() throws IOException {
        final Random rng = new Random(12345);
        final TextAndNubbins tns = buildTable(rng, 1000, 1);

        final org.supercsv.io.CsvListReader csvReader = new org.supercsv.io.CsvListReader(new StringReader(tns.text),
                org.supercsv.prefs.CsvPreference.STANDARD_PREFERENCE);
        final TIntArrayList results = new TIntArrayList();
        if (csvReader.read() == null) {
            throw new RuntimeException("Expected header row");
        }
        while (true) {
            final List<String> next = csvReader.read();
            if (next == null) {
                break;
            }
            results.add(Integer.parseInt(next.get(0)));
        }
        final int[] typedData = results.toArray();
        Assertions.assertThat(typedData).isEqualTo(tns.nubbins[0]);
    }


    public void univocity() {
        final Random rng = new Random(12345);
        final TextAndNubbins tns = buildTable(rng, 1000, 1);

        final com.univocity.parsers.csv.CsvParserSettings settings = new com.univocity.parsers.csv.CsvParserSettings();
        settings.setNullValue("");
        final com.univocity.parsers.csv.CsvParser parser = new com.univocity.parsers.csv.CsvParser(settings);
        parser.beginParsing(new StringReader(tns.text));

        if (parser.parseNext() == null) {
            throw new RuntimeException("Expected header row");
        }
        final TIntArrayList results = new TIntArrayList();
        while (true) {
            final String[] next = parser.parseNext();
            if (next == null) {
                break;
            }
            results.add(Integer.parseInt(next[0]));
        }
        final int[] typedData = results.toArray();
        Assertions.assertThat(typedData).isEqualTo(tns.nubbins[0]);
    }


    public interface ResultProvider<TARRAY> {
        TARRAY toResult();
    }

    private static abstract class MySinkBase<TCOLLECTION, TARRAY> implements io.deephaven.csv.sinks.Sink<TARRAY>, ResultProvider<TARRAY> {
        protected final TCOLLECTION collection;
        protected int collectionSize;
        protected final FillOperation<TCOLLECTION> fillOperation;
        protected final SetOperation<TCOLLECTION, TARRAY> setOperation;
        protected final AddOperation<TCOLLECTION, TARRAY> addOperation;
        protected final Function<TCOLLECTION, TARRAY> toResultOperation;

        protected MySinkBase(TCOLLECTION collection, FillOperation<TCOLLECTION> fillOperation,
                             SetOperation<TCOLLECTION, TARRAY> setOperation, AddOperation<TCOLLECTION, TARRAY> addOperation,
                             Function<TCOLLECTION, TARRAY> toResultOperation) {
            this.collection = collection;
            this.fillOperation = fillOperation;
            this.setOperation = setOperation;
            this.addOperation = addOperation;
            this.toResultOperation = toResultOperation;
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

        public final TARRAY toResult() {
            return toResultOperation.apply(collection);
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
            implements io.deephaven.csv.sinks.Source<TARRAY>, io.deephaven.csv.sinks.Sink<TARRAY> {
        private final ReadToArrayOperation<TCOLLECTION, TARRAY> readToArrayOperation;

        protected MySourceAndSinkBase(TCOLLECTION collection, FillOperation<TCOLLECTION> fillOperation,
                                      SetOperation<TCOLLECTION, TARRAY> setOperation, AddOperation<TCOLLECTION, TARRAY> addOperation,
                                      Function<TCOLLECTION, TARRAY> toResultOperation,
                                      ReadToArrayOperation<TCOLLECTION, TARRAY> readToArrayOperation) {
            super(collection, fillOperation, setOperation, addOperation, toResultOperation);
            this.readToArrayOperation = readToArrayOperation;
        }

        @Override
        public void read(TARRAY dest, boolean[] isNull, long srcBegin, long srcEnd) {
            if (srcBegin == srcEnd) {
                return;
            }
            final int size = Math.toIntExact(srcEnd - srcBegin);
            readToArrayOperation.apply(collection, dest, Math.toIntExact(srcBegin), 0, size);
        }

        /**
         * Meant to be paired with e.g. TDoubleArrayList.add(double[] dest, int source_pos, int dest_pos, int length)
         */
        private interface ReadToArrayOperation<TCOLLECTION, TARRAY> {
            void apply(TCOLLECTION coll, TARRAY dest, int source_pos_, int dest_pos, int length);
        }
    }

    private static class MyByteSinkBase extends MySourceAndSinkBase<TByteArrayList, byte[]> {
        public MyByteSinkBase() {
            super(new TByteArrayList(),
                    (dest, from, to) -> dest.fill(from, to, (byte) 0),
                    TByteArrayList::set,
                    TByteArrayList::add,
                    TByteArrayList::toArray,
                    TByteArrayList::toArray);  // Note: different "toArray" from the above.
        }
    }

    private static final class MyByteSink extends MyByteSinkBase {
    }

    private static final class MyShortSink extends MySourceAndSinkBase<TShortArrayList, short[]> {
        public MyShortSink() {
            super(new TShortArrayList(),
                    (dest, from, to) -> dest.fill(from, to, (short) 0),
                    TShortArrayList::set,
                    TShortArrayList::add,
                    TShortArrayList::toArray,
                    TShortArrayList::toArray);  // Note: different "toArray" from the above.
        }
    }

    private static final class MyIntSink extends MySourceAndSinkBase<TIntArrayList, int[]> {
        public MyIntSink() {
            super(new TIntArrayList(),
                    (dest, from, to) -> dest.fill(from, to, 0),
                    TIntArrayList::set,
                    TIntArrayList::add,
                    TIntArrayList::toArray,
                    TIntArrayList::toArray);  // Note: different "toArray" from the above.
        }
    }

    private static class MyLongSinkBase extends MySourceAndSinkBase<TLongArrayList, long[]> {
        public MyLongSinkBase() {
            super(new TLongArrayList(),
                    (dest, from, to) -> dest.fill(from, to, 0L),
                    TLongArrayList::set,
                    TLongArrayList::add,
                    TLongArrayList::toArray,
                    TLongArrayList::toArray);  // Note: different "toArray" from the above.
        }
    }

    private static final class MyLongSink extends MyLongSinkBase {
    }

    private static final class MyFloatSink extends MySinkBase<TFloatArrayList, float[]> {
        public MyFloatSink() {
            super(new TFloatArrayList(),
                    (dest, from, to) -> dest.fill(from, to, 0),
                    TFloatArrayList::set,
                    TFloatArrayList::add,
                    TFloatArrayList::toArray);
        }
    }

    private static final class MyDoubleSink extends MySinkBase<TDoubleArrayList, double[]> {
        public MyDoubleSink() {
            super(new TDoubleArrayList(),
                    (dest, from, to) -> dest.fill(from, to, 0),
                    TDoubleArrayList::set,
                    TDoubleArrayList::add,
                    TDoubleArrayList::toArray);
        }
    }

    private static final class MyBooleanAsByteSink extends MyByteSinkBase {
    }

    private static final class MyCharSink extends MySinkBase<TCharArrayList, char[]> {
        public MyCharSink() {
            super(new TCharArrayList(),
                    (coll, from, to) -> coll.fill(from, to, (char) 0),
                    TCharArrayList::set,
                    TCharArrayList::add,
                    TCharArrayList::toArray);
        }
    }

    private static final class MyStringSink extends MySinkBase<ArrayList<String>, String[]> {
        public MyStringSink() {
            super(new ArrayList<>(),
                    MyStringSink::fill,
                    MyStringSink::set,
                    MyStringSink::add,
                    c -> c.toArray(new String[0]));
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
    }

    private static final class MyTimestampAsLongSink extends MyLongSinkBase {
    }

    private static io.deephaven.csv.sinks.SinkFactory makeMySinkFactory() {
        return io.deephaven.csv.sinks.SinkFactory.of(
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
