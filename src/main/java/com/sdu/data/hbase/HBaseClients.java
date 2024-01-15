package com.sdu.data.hbase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseClients {

    private static final Connection connection;

    static {
        Configuration configuration = HBaseConfiguration.create();
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            throw new RuntimeException("failed initialize hbase client", e);
        }
    }

    public static void createTable(String tableName, List<String> columns) {
        try {
            // use default namespace
            TableName table = TableName.valueOf(tableName);
            Admin admin = connection.getAdmin();
            if (admin.tableExists(table)) {
                return;
            }
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(table);
            columns.stream().map(ColumnFamilyDescriptorBuilder::of).forEach(builder::setColumnFamily);
            admin.createTable(builder.build());
        } catch (IOException e) {
            throw new RuntimeException("failed create hbase table, name: " + tableName, e);
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

    public static Result get(String tableName, String key) {
        try (Table table = connection.getTable(TableName.valueOf(tableName))) {
            Get get = new Get(Bytes.toBytes(key));
            return table.get(get);
        } catch (IOException e) {
            throw new RuntimeException("failed get data from hbase, name: " + tableName + " and key: " + key, e);
        }
    }
}
