package com.sdu.data.test.hbase;

import static com.sdu.data.common.JsonUtils.toJson;
import static com.sdu.data.hbase.HBaseRowDataConverter.SIMPLE_COLUMN_CONVERTER;
import static java.lang.String.format;
import static org.apache.hadoop.hbase.HConstants.REGIONINFO_QUALIFIER_STR;
import static org.apache.hadoop.hbase.HConstants.SEQNUM_QUALIFIER_STR;
import static org.apache.hadoop.hbase.HConstants.SERVERNAME_QUALIFIER_STR;
import static org.apache.hadoop.hbase.HConstants.SERVER_QUALIFIER_STR;
import static org.apache.hadoop.hbase.HConstants.STARTCODE_QUALIFIER_STR;
import static org.apache.hadoop.hbase.HConstants.STATE_QUALIFIER_STR;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.sdu.data.hbase.HBaseClients;
import com.sdu.data.hbase.HBaseRowData;
import com.sdu.data.hbase.HBaseRowDataConverter;

public class HBaseClientsTest {

    private static final String SYSTEM_NAMESPACE = "hbase";
    // meta
    private static final String META_TABLE_QUALIFIER = "meta";
    private static final String META_TABLE_NAME = format("%s:%s", SYSTEM_NAMESPACE, META_TABLE_QUALIFIER);
    private static final String META_COLUMN_FAMILY_INFO = "info";
    private static final String META_COLUMN_FAMILY_TABLE = "table";
    private static final String META_COLUMN_FAMILY_REP_BARRIER = "rep_barrier";

    // namespace
    private static final String NAMESPACE_TABLE_QUALIFIER = "namespace";
    private static final String NAMESPACE_TABLE_NAME = format("%s:%s", SYSTEM_NAMESPACE, NAMESPACE_TABLE_QUALIFIER);
    private static final String NAMESPACE_COLUMN_FAMILY_INFO = "info";

    // video
    private static final String VIDEO_NAMESPACE = "video";
    private static final String VIDEO_VOD_TABLE_QUALIFIER = "vod";
    private static final String VIDEO_LIVE_TABLE_QUALIFIER = "live";
    private static final String VIDEO_VOD_TABLE_NAME = format("%s:%s", VIDEO_NAMESPACE, VIDEO_VOD_TABLE_QUALIFIER);
    private static final String VIDEO_LIVE_TABLE_NAME = format("%s:%s", VIDEO_NAMESPACE, VIDEO_LIVE_TABLE_QUALIFIER);
    private static final List<String> VIDEO_COLUMN_FAMILIES = Lists.newArrayList("product", "transcode", "play");

    @Test
    public void testCreateTable() {
        boolean success = HBaseClients.createTable(VIDEO_VOD_TABLE_NAME, VIDEO_COLUMN_FAMILIES);
        Assert.assertTrue(success);

        success = HBaseClients.createTable(VIDEO_LIVE_TABLE_NAME, VIDEO_COLUMN_FAMILIES);
        Assert.assertTrue(success);
    }

    @Test
    public void testGetMetaTableDescriptor() {
        Optional<TableDescriptor> descriptorOpt = HBaseClients.listTableDescriptor(META_TABLE_NAME);
        Assert.assertTrue(descriptorOpt.isPresent());
        printTableDescriptorDetail(descriptorOpt.get());
    }

    @Test
    public void testGetMetaTable() {
        // hbase:meta:table:state
        HBaseRowDataConverter<HBaseRowData<String, String, String, String>> tableConverter =
                (rowKey, family, qualifier, value) -> HBaseRowData.of(
                        Bytes.toString(rowKey),
                        Bytes.toString(family),
                        Bytes.toString(qualifier),
                        HBaseProtos.TableState.parseFrom(value).getState().name()
                );
        List<HBaseRowData<String, String, String, String>> metaTableValues =
                HBaseClients.scan(
                        META_TABLE_NAME,
                        META_COLUMN_FAMILY_TABLE,
                        tableConverter);
        printTableRowData(metaTableValues);
        printSeparator();

        // hbase:meta:info
        HBaseRowDataConverter<HBaseRowData<String, String, String, String>> infoConverter =
                (rowKey, family, qualifier, value) -> {
                    String columnQualifier = Bytes.toString(qualifier);
                    String formatValue;
                    switch (columnQualifier) {
                        case REGIONINFO_QUALIFIER_STR: // hbase:meta:info:regioninfo
                            RegionInfo regionInfo = RegionInfo.parseFrom(value);
                            formatValue = toJson(regionInfo);
                            break;
                        case SEQNUM_QUALIFIER_STR:     // hbase:meta:info:seqnumDuringOpen
                        case STARTCODE_QUALIFIER_STR:  // hbase:meta:info:serverstartcode
                            formatValue = String.valueOf(Bytes.toLong(value));
                            break;
                        case SERVER_QUALIFIER_STR:     // hbase:meta:info:server
                        case SERVERNAME_QUALIFIER_STR: // hbase:meta:info:sn
                        case STATE_QUALIFIER_STR:      // hbase:meta:info:state
                            formatValue = Bytes.toString(value);
                            break;
                        default:
                            throw new RuntimeException("unknown column qualifier: " + columnQualifier);
                    }
                    return HBaseRowData.of(
                            Bytes.toString(rowKey),
                            Bytes.toString(family),
                            columnQualifier,
                            formatValue
                    );
                };
        List<HBaseRowData<String, String, String, String>> metaInfoValues =
                HBaseClients.scan(
                        META_TABLE_NAME,
                        META_COLUMN_FAMILY_INFO,
                        infoConverter);
        printTableRowData(metaInfoValues);
        printSeparator();


        // hbase:meta:rep_barrier
        List<HBaseRowData<String, String, String, String>> metaRepBarrierValues =
                HBaseClients.scan(
                        META_TABLE_NAME,
                        META_COLUMN_FAMILY_REP_BARRIER,
                        SIMPLE_COLUMN_CONVERTER);
        printTableRowData(metaRepBarrierValues);
    }

    @Test
    public void testGetNamespaceTableDescriptor() {
        Optional<TableDescriptor> descriptorOpt = HBaseClients.listTableDescriptor(NAMESPACE_TABLE_NAME);
        Assert.assertTrue(descriptorOpt.isPresent());
        printTableDescriptorDetail(descriptorOpt.get());

    }

    @Test
    public void testGetNamespaceTable() {
        // hbase:namespace:info
        HBaseRowDataConverter<HBaseRowData<String, String, String, String>> infoConverter =
                (rowKey, family, qualifier, value) -> {
                    NamespaceDescriptor descriptor = ProtobufUtil.toNamespaceDescriptor(
                            org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.NamespaceDescriptor.parseFrom(value)
                    );
                    return HBaseRowData.of(
                            Bytes.toString(rowKey),
                            Bytes.toString(family),
                            Bytes.toString(qualifier),
                            toJson(descriptor)
                    );
                };
        List<HBaseRowData<String, String, String, String>> namespaceInfoValues =
                HBaseClients.scan(
                        NAMESPACE_TABLE_NAME,
                        NAMESPACE_COLUMN_FAMILY_INFO,
                        infoConverter);
        printTableRowData(namespaceInfoValues);
    }

    private static void printTableDescriptorDetail(TableDescriptor descriptor) {
        for (ColumnFamilyDescriptor columnFamilyDescriptor : descriptor.getColumnFamilies()) {
            String columnFamilyName = columnFamilyDescriptor.getNameAsString();
            System.out.println("column family name: " + columnFamilyName);
            for (Map.Entry<Bytes, Bytes> valueEntry : columnFamilyDescriptor.getValues().entrySet()) {
                String colKey = valueEntry.getKey().toString();
                String colVal = valueEntry.getValue().toString();
                System.out.println(" key: " + colKey + ", column value: " + colVal);
            }
        }
        printSeparator();
    }

    private static void printTableRowData(List<HBaseRowData<String, String, String, String>> rowDataList) {
        if (rowDataList == null || rowDataList.isEmpty()) {
            return;
        }
        for (HBaseRowData<String, String, String, String> data : rowDataList) {
            String column = format("%s:%s", data.getFamily(), data.getQualifier());
            String msg = format("key = %s, colum = %s, value = %s", data.getKey(), column, data.getValue());
            System.out.println(msg);
        }
    }

    private static void printSeparator() {
        System.out.println("=========================================================");
    }

}
