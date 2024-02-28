package com.sdu.data.hbase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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

    public static void put(String tableName, String key, Tuple3<String, String, byte[]> ... columns) {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Put put = new Put(Bytes.toBytes(key));
            // column family, column name, column value
            Arrays.stream(columns).forEach(tuple -> put.addColumn(Bytes.toBytes(tuple.getT1()), Bytes.toBytes(tuple.getT2()), tuple.getT3()));
            table.put(put);
        } catch (IOException e) {
            throw new RuntimeException("failed put data to hbase table, name: " + tableName, e);
        }
    }

    public static <RESULT> List<RESULT> scan(String tableName, String columnFamily, ColumnConverter<RESULT> columnConverter) {
        try {
            if (tableName == null || tableName.isEmpty()) {
                return Collections.emptyList();
            }
            if (columnFamily == null || columnFamily.isEmpty()) {
                return Collections.emptyList();
            }
            Optional<Table> tableOpt = getTable(tableName);
            Table table = tableOpt.orElse(null);
            if (table == null) {
                return Collections.emptyList();
            }
            byte[] family = Bytes.toBytes(columnFamily);

            try (ResultScanner rs = table.getScanner(family)) {
                List<RESULT> values = new LinkedList<>();
                Iterator<Result> it = rs.iterator();
                while (it.hasNext()) {
                    Result result = it.next();
                    byte[] rowKey = result.getRow();
                    // 列族的所有的列限定符
                    Map<byte[], byte[]> qualifiers = result.getFamilyMap(family);
                    for (Map.Entry<byte[], byte[]> qualifierEntry : qualifiers.entrySet()) {
                        values.add(columnConverter.convert(rowKey, family, qualifierEntry.getKey(), qualifierEntry.getValue()));
                    }
                }
                return values;
            }
        } catch (Exception e) {
            throw new RuntimeException("failed scan table data, tableName: " + tableName + ", columFamily: " + columnFamily, e);
        }
    }

    public static Result get(String tableName, String key) {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(Bytes.toBytes(key));
            return table.get(get);
        } catch (IOException e) {
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
