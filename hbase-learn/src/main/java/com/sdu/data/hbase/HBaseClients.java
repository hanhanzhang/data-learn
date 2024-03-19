package com.sdu.data.hbase;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseClients {

    private static final Connection connection;

    static {
        Configuration configuration = HBaseConfiguration.create();
        // load configuration
        configuration.addResource("/Users/hanhan.zhang/software/hbase/conf/hbase-site.xml");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            throw new RuntimeException("failed initialize hbase client", e);
        }
    }

    /*
     *  1. tableName pattern有两种:
     *     a. ${namespace}:${table qualifier}
     *     b. ${table qualifier}
     *
     *  2. Admin.createTable()涉及的RPC接口:
     * */
    public static boolean createTable(String tableName, List<String> columnFamilies) {
        if (tableName == null || tableName.isEmpty()) {
            return false;
        }
        if (columnFamilies == null || columnFamilies.isEmpty()
                || columnFamilies.stream().anyMatch(columnFamily -> columnFamily == null || columnFamily.isEmpty())) {
            return false;
        }
        TableName table = TableName.valueOf(tableName);
        List<ColumnFamilyDescriptor> columnFamilyDescriptors =
                columnFamilies.stream().map(ColumnFamilyDescriptorBuilder::of).collect(Collectors.toList());
        return createTable(table, columnFamilyDescriptors);
    }

    public static boolean createTable(TableName table, List<ColumnFamilyDescriptor> columnFamilies) {
        try {
            Admin admin = connection.getAdmin();
            if (admin.tableExists(table)) {
                return true;
            }
            if (!checkNamespaceIfExist(table)) {
                NamespaceDescriptor descriptor = NamespaceDescriptor.create(table.getNamespaceAsString()).build();
                admin.createNamespace(descriptor);
            }
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(table);
            columnFamilies.forEach(builder::setColumnFamily);
            admin.createTable(builder.build());
            return true;
        } catch (IOException e) {
            throw new RuntimeException("failed create hbase table, name: " + table.getNameWithNamespaceInclAsString(), e);
        }
    }

    public static Optional<TableDescriptor> listTableDescriptor(String tableName) {
        try {
            Admin admin = connection.getAdmin();
            return Optional.of(admin.getDescriptor(TableName.valueOf(tableName)));
        } catch (TableNotFoundException e) {
            return Optional.empty();
        } catch (IOException e) {
            throw new RuntimeException("failed get hbase table descriptor, name: " + tableName, e);
        }
    }

    public static Optional<Table> getTable(String tableName) {
        try {
            return Optional.of(connection.getTable(TableName.valueOf(tableName)));
        } catch (TableNotFoundException e) {
            return Optional.empty();
        } catch (IOException e) {
            throw new RuntimeException("failed get hbase table, name: " + tableName, e);
        }
    }

    public static void dropTable(String tableName) {
        try {
            Admin admin = connection.getAdmin();
            admin.deleteTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            throw new RuntimeException("failed drop hbase table, name: " + tableName, e);
        }
    }

    public static void put(HPutParam putParam) {
        try (Table table = connection.getTable(TableName.valueOf(putParam.getTableName()))) {
            final Put put = new Put(Bytes.toBytes(putParam.getRowKey()));
            for (HPutParam.PutColumnParam columnParam : putParam.getColumnPutParams()) {
                for (Map.Entry<String, byte[]> qualifierEntry : columnParam.getQualifierValues().entrySet()) {
                    put.addColumn(
                            Bytes.toBytes(columnParam.getFamily()),
                            Bytes.toBytes(qualifierEntry.getKey()),
                            qualifierEntry.getValue()
                    );
                }
            }
            // column family, column name, column value
            table.put(put);
        } catch (IOException e) {
            throw new RuntimeException("failed put data to hbase table, name: " + putParam.getTableName(), e);
        }
    }

    public static <RESULT> List<RESULT> scan(String tableName, String columnFamily, HBaseRowDataConverter<RESULT> rowConverter) {
        try {
            checkArgument(tableName != null && !tableName.isEmpty());
            checkArgument(columnFamily != null && !columnFamily.isEmpty());
            Optional<Table> tableOpt = getTable(tableName);
            Table table = tableOpt.orElse(null);
            if (table == null) {
                return Collections.emptyList();
            }
            byte[] family = Bytes.toBytes(columnFamily);

            try (ResultScanner rs = table.getScanner(family)) {
                List<RESULT> values = new LinkedList<>();
                for (Result result : rs) {
                    byte[] rowKey = result.getRow();
                    // 列族的所有的列限定符
                    Map<byte[], byte[]> qualifiers = result.getFamilyMap(family);
                    for (Map.Entry<byte[], byte[]> qualifierEntry : qualifiers.entrySet()) {
                        values.add(rowConverter.convert(rowKey, family, qualifierEntry.getKey(), qualifierEntry.getValue()));
                    }
                }
                return values;
            }
        } catch (Exception e) {
            throw new RuntimeException("failed scan table data, tableName: " + tableName + ", columFamily: " + columnFamily, e);
        }
    }

    public static <RESULT> List<RESULT> get(String tableName, String key, HBaseRowDataConverter<RESULT> rowConverter) {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(Bytes.toBytes(key));
            Result result = table.get(get);
            byte[] rowKey = result.getRow();
            List<RESULT> values = new LinkedList<>();
            // 列族的所有的列限定符
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyMap = result.getMap();
            for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> family : familyMap.entrySet()) {
                byte[] familyBytes = family.getKey();
                for (Map.Entry<byte[], NavigableMap<Long, byte[]>> qualifier : family.getValue().entrySet()) {
                    byte[] qualifierBytes = qualifier.getKey();
                    NavigableMap<Long, byte[]> versionValues = qualifier.getValue();
                    values.add(
                            rowConverter.convert(
                                    rowKey,
                                    familyBytes,
                                    qualifierBytes,
                                    versionValues.get(qualifier.getValue().firstKey()) // 取最新版本
                            )
                    );
                }
            }
            return values;
        } catch (Exception e) {
            throw new RuntimeException("failed get data from hbase, name: " + tableName + " and key: " + key, e);
        }
    }


    private static boolean checkNamespaceIfExist(TableName tableName) {
        try {
            Admin admin = connection.getAdmin();
            NamespaceDescriptor descriptor = admin.getNamespaceDescriptor(tableName.getNamespaceAsString());
            return descriptor != null && descriptor.getName().equalsIgnoreCase(tableName.getNamespaceAsString());
        } catch (NamespaceNotFoundException e) {
            return false;
        } catch (IOException e) {
            throw new RuntimeException("failed check namespace", e);
        }
    }

}
