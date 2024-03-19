package com.sdu.data.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.sdu.data.common.SerializerInfo;

public class RegionInfoSerializer extends JsonSerializer<RegionInfo> implements SerializerInfo<RegionInfo> {

    @Override
    public void serialize(RegionInfo value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeNumberField("regionId", value.getRegionId());
        gen.writeNumberField("replicaId", value.getReplicaId());
        gen.writeBooleanField("offline", value.isOffline());
        gen.writeBooleanField("metaRegion", value.isMetaRegion());
        gen.writeBooleanField("first", value.isFirst());
        gen.writeBooleanField("last", value.isLast());
        gen.writeBooleanField("split", value.isSplit());
        gen.writeBooleanField("splitParent", value.isSplitParent());
        gen.writeBooleanField("degenerate", value.isDegenerate());

        gen.writeStringField("regionName", value.getRegionNameAsString());
        gen.writeStringField("encodedName", value.getEncodedName());
        gen.writeStringField("startKey", Bytes.toString(value.getStartKey()));
        gen.writeStringField("endKey", Bytes.toString(value.getStartKey()));

        gen.writeObjectFieldStart("tableName");
        gen.writeStringField("namespace", value.getTable().getNamespaceAsString());
        gen.writeStringField("qualifier", value.getTable().getQualifierAsString());
        gen.writeBooleanField("systemTable", value.getTable().isSystemTable());
        gen.writeEndObject();

        gen.writeEndObject();

    }

    @Override
    public Class<RegionInfo> serializerClass() {
        return RegionInfo.class;
    }
}
