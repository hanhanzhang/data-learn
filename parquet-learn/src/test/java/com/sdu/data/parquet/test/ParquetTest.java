package com.sdu.data.parquet.test;

import static com.sdu.data.parquet.type.Types.doubleType;
import static com.sdu.data.parquet.type.Types.intType;
import static com.sdu.data.parquet.type.Types.listType;
import static com.sdu.data.parquet.type.Types.mapType;
import static com.sdu.data.parquet.type.Types.rowType;
import static com.sdu.data.parquet.type.Types.stringType;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.sdu.data.parquet.RowData;
import com.sdu.data.parquet.type.RowType;
import com.sdu.data.parquet.type.Type;
import com.sdu.data.parquet.writer.RowDataWriteSupport;

public class ParquetTest {

    private Path path;

    @Before
    public void setup() {
        path = new Path("target/test/test.parquet");
    }


    @Test
    public void testWriteWithBasicType() throws Exception {
        RowType rowType = new RowType(
                new String[] {"id", "name", "age", "chinese_score", "math_score"},
                new Type[] {intType(false), stringType(false), intType(false), doubleType(false), doubleType(false)});
        RowData[] rows = new RowData[] {
                new RowData(rowType, new Object[] {1, "james", 12, 99.5, 98.0}),
                new RowData(rowType, new Object[] {2, "lix", 12, 88.0, 87.5}),
                new RowData(rowType, new Object[] {3, "thane", 12, 77.5, 79.5}),
                new RowData(rowType, new Object[] {4, "jada", 12, 66.5, 66.0}),
                new RowData(rowType, new Object[] {5, "mde", 13, 55.0, 59.5}),
                new RowData(rowType, new Object[] {6, "mimy", 13, 45.0, 41.0}),
        };

        write(rowType, rows, path);
        List<GenericRecord> records = readRecords(path);

        Assert.assertEquals(rows.length, records.size());

        for (GenericRecord record : records) {
            System.out.println(record);
        }
    }

    @Test
    public void testWriteWithBasicAndListType() throws Exception {
        RowType rowType = new RowType(
                new String[] {"id", "name", "age", "scores"},
                new Type[] {intType(false), stringType(false), intType(false), listType(false, doubleType(false))});
        RowData[] rows = new RowData[] {
                new RowData(rowType, new Object[] {1, "james", 12, new Double[] {99.5, 98.0}}),
                new RowData(rowType, new Object[] {2, "lix", 12, new Double[] {88.0, 87.5}}),
                new RowData(rowType, new Object[] {3, "thane", 12, new Double[] {77.5, 79.5}}),
                new RowData(rowType, new Object[] {4, "jada", 12, new Double[] {66.5, 66.0}}),
                new RowData(rowType, new Object[] {5, "mde", 13, new Double[] {55.0, 59.5}}),
                new RowData(rowType, new Object[] {6, "mimy", 13, new Double[] {45.0, 41.0}}),
        };

        write(rowType, rows, path);
        List<GenericRecord> records = readRecords(path);

        Assert.assertEquals(rows.length, records.size());

        for (GenericRecord record : records) {
            System.out.println(record);
        }
    }

    @Test
    public void testWriteWithBasicAndMapType() throws Exception {
        RowType rowType = new RowType(
                new String[] {"id", "name", "age", "parents"},
                new Type[] {intType(false), stringType(false), intType(false), mapType(false,stringType(false), stringType(false))}
        );
        RowData[] rows = new RowData[] {
                new RowData(rowType, new Object[] {1, "james", 12, initMap(Pair.of("m_james", "f_james"))}),
                new RowData(rowType, new Object[] {2, "lix", 12, initMap(Pair.of("m_lix", "f_lix"))}),
                new RowData(rowType, new Object[] {3, "thane", 13, initMap(Pair.of("m_thane", "m_thane"))}),
                new RowData(rowType, new Object[] {4, "jada", 12, initMap(Pair.of("m_jada", "f_jada"))}),
                new RowData(rowType, new Object[] {5, "mde", 12, initMap(Pair.of("m_mde", "f_mde"))}),
                new RowData(rowType, new Object[] {6, "mimy", 12, initMap(Pair.of("m_mimy", "f_mimy"))}),
        };

        write(rowType, rows, path);
        List<GenericRecord> records = readRecords(path);

        Assert.assertEquals(rows.length, records.size());

        for (GenericRecord record : records) {
            System.out.println(record);
        }
    }

    @Test
    public void testWriteWithNestRowType() throws Exception {
        RowType parentRowType = rowType(false, new String[] {"parent_type", "parent_name"}, new Type[] {intType(false), stringType(false)});
        RowType rowType = new RowType(
                new String[] {"id", "name", "parents"},
                new Type[] {intType(false), stringType(false), listType(false, parentRowType)}
        );
        RowData[] rows = new RowData[] {
                new RowData(rowType, new Object[] {1, "james",
                        new RowData[] {
                                new RowData(parentRowType, new Object[] {1, "m_james"}),
                                new RowData(parentRowType, new Object[] {2, "f_james"})}}),
                new RowData(rowType, new Object[] {2, "lix",
                        new RowData[] {
                                new RowData(parentRowType, new Object[] {1, "m_lix"}),
                                new RowData(parentRowType, new Object[] {2, "f_lix"})}}),
                new RowData(rowType, new Object[] {3, "thane",
                        new RowData[] {
                                new RowData(parentRowType, new Object[] {1, "m_thane"}),
                                new RowData(parentRowType, new Object[] {2, "f_thane"})}}),
                new RowData(rowType, new Object[] {4, "jada", new RowData[] {
                        new RowData(parentRowType, new Object[] {1, "m_jada"}),
                        new RowData(parentRowType, new Object[] {2, "f_jada"})}}),
                new RowData(rowType, new Object[] {5, "mde", new RowData[] {
                        new RowData(parentRowType, new Object[] {1, "m_mde"}),
                        new RowData(parentRowType, new Object[] {2, "f_mde"})}}),
                new RowData(rowType, new Object[] {6, "mimy",new RowData[] {
                        new RowData(parentRowType, new Object[] {1, "m_mimy"}),
                        new RowData(parentRowType, new Object[] {2, "f_mimy"})}}),
        };

        write(rowType, rows, path);
        List<GenericRecord> records = readRecords(path);

        Assert.assertEquals(rows.length, records.size());

        for (GenericRecord record : records) {
            System.out.println(record);
        }
    }

    @Test
    public void testWriteWithAllTypes() throws Exception {
        RowType rowType = new RowType(
                new String[] {"id", "name", "scores", "parents"},
                new Type[] {intType(false), stringType(false), listType(false, intType(false)), mapType(false, stringType(false), stringType(false))}
        );

        RowData[] rows = new RowData[] {
                new RowData(rowType, new Object[] {1, "james", new Integer[] {90, 90}, initMap(Pair.of("m_james", "f_james"))}),
                new RowData(rowType, new Object[] {2, "lix", new Integer[] {88, 85}, initMap(Pair.of("m_lix", "f_lix"))}),
                new RowData(rowType, new Object[] {3, "thane", new Integer[] {70, 78}, initMap(Pair.of("m_thane", "m_thane"))}),
                new RowData(rowType, new Object[] {4, "jada", new Integer[] {67, 68}, initMap(Pair.of("m_jada", "f_jada"))}),
                new RowData(rowType, new Object[] {5, "mde", new Integer[] {54, 53}, initMap(Pair.of("m_mde", "f_mde"))}),
                new RowData(rowType, new Object[] {6, "mimy", new Integer[] {46, 46}, initMap(Pair.of("m_mimy", "f_mimy"))}),
        };

        write(rowType, rows, path);
        List<GenericRecord> records = readRecords(path);

        Assert.assertEquals(rows.length, records.size());

        for (GenericRecord record : records) {
            System.out.println(record);
        }
    }

    private static List<GenericRecord> readRecords(Path path) throws Exception {
        List<GenericRecord> records = new LinkedList<>();

        try(ParquetReader<GenericRecord> avroRecordParquetReader = ParquetReader.<GenericRecord>builder(new AvroReadSupport<>(), path).build()) {
            GenericRecord record;
            while ((record = avroRecordParquetReader.read()) != null) {
                records.add(record);
            }
        }

        return records;
    }
    private static void write(RowType rowType, RowData[] rows, Path path) throws Exception {
        try(ParquetWriter<RowData> parquetWriter = new ParquetWriterBuilder(path).withRowType(rowType).withWriteMode(ParquetFileWriter.Mode.OVERWRITE).build()) {
            for (RowData data : rows) {
                parquetWriter.write(data);
            }
        }
    }

    private static class ParquetWriterBuilder extends ParquetWriter.Builder<RowData, ParquetWriterBuilder> {

        private RowType rowType;

        public ParquetWriterBuilder(Path path) {
            super(path);
        }

        @Override
        protected ParquetWriterBuilder self() {
            return this;
        }

        public ParquetWriterBuilder withRowType(RowType rowType) {
            this.rowType = rowType;
            return this;
        }

        @Override
        protected WriteSupport<RowData> getWriteSupport(Configuration conf) {
            return new RowDataWriteSupport(rowType);
        }
    }

    private static Map<String, String> initMap(Pair<String, String> pair) {
        Map<String, String> map = new HashMap<>();
        map.put("mother", pair.getLeft());
        map.put("father", pair.getRight());
        return map;
    }

}
