package com.sdu.data.parquet.reader;

import java.util.LinkedList;
import java.util.List;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;

import com.google.common.base.Preconditions;
import com.sdu.data.parquet.ParquetSchemas;
import com.sdu.data.parquet.RowData;
import com.sdu.data.type.RowType;
import com.sdu.data.type.Type;

public class RowDataConvert extends GroupConverter {

    private final RowDataContainer rootContainer;
    private RowData currentRecord;

    private final Converter[] converters;

    public RowDataConvert(MessageType schema) {
        RowType rowType = ParquetSchemas.convertRowType(schema, false);
        this.rootContainer = new RowDataContainerImpl(rowType);
        this.converters = new Converter[schema.getFieldCount()];
        for (int i = 0; i < schema.getFieldCount(); i++) {
            Type type = rowType.getFieldType(i);
            converters[i] = createTypeConvert(type, rootContainer.getFieldValueContainer(i));
        }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return converters[fieldIndex];
    }

    @Override
    public void start() {

    }

    @Override
    public void end() {
        currentRecord = (RowData) rootContainer.getValue();
    }

    public RowData getCurrentRecord() {
        return currentRecord;
    }

    private static Converter createTypeConvert(Type type, ValueContainer container) {
        switch (type.type()) {
            case INT:
                return new IntegerTypeConvert(container.as(IntValueContainer.class));
            case FLOAT:
                return new FloatTypeConvert(container.as(FloatValueContainer.class));
            case DOUBLE:
                return new DoubleTypeConvert(container.as(DoubleValueContainer.class));
            case STRING:
                return new StringTypeConvert(container.as(StringValueContainer.class));
            case BOOLEAN:
                return new BooleanTypeConvert(container.as(BooleanValueContainer.class));
            case LIST:
                return new ListTypeConvert(container.as(ListValueContainer.class));
            case MAP:
            case ROW:
            default:
                throw new RuntimeException("");
        }
    }

    public static class ListTypeConvert extends GroupConverter {

        private final ListValueContainer parent;
        private final List<ValueContainer> elementContainers;

        public ListTypeConvert(ListValueContainer container) {
            this.parent = container;
            this.elementContainers = new LinkedList<>();
        }

        @Override
        public Converter getConverter(int fieldIndex) {
            Preconditions.checkArgument(fieldIndex == 0);
            ValueContainer elementValueContainer = parent.getElementValueContainer();
            this.elementContainers.add(elementValueContainer);
            return createTypeConvert(parent.getElementType(), elementValueContainer);
        }

        @Override
        public void start() {

        }

        @Override
        public void end() {
            for (ValueContainer elementContainer : elementContainers) {
                this.parent.addValue(elementContainer.getValue());
                elementContainer.reset();
            }
        }
    }

    public static class IntegerTypeConvert extends PrimitiveConverter {

        private final IntValueContainer container;

        public IntegerTypeConvert(IntValueContainer container) {
            this.container = container;
        }

        @Override
        public void addInt(int value) {
            container.addInteger(value);
        }
    }

    public static class FloatTypeConvert extends PrimitiveConverter {

        private final FloatValueContainer container;

        public FloatTypeConvert(FloatValueContainer container) {
            this.container = container;
        }

        @Override
        public void addFloat(float value) {
            container.addFloat(value);
        }
    }

    public static class DoubleTypeConvert extends PrimitiveConverter {

        private final DoubleValueContainer container;

        public DoubleTypeConvert(DoubleValueContainer container) {
            this.container = container;
        }

        @Override
        public void addDouble(double value) {
            container.addDouble(value);
        }
    }

    public static class StringTypeConvert extends PrimitiveConverter {

        private final StringValueContainer container;

        public StringTypeConvert(StringValueContainer container) {
            this.container = container;
        }

        @Override
        public void addBinary(Binary value) {
            container.addString(value.toStringUsingUTF8());
        }
    }

    public static class BooleanTypeConvert extends PrimitiveConverter {

        private final BooleanValueContainer container;

        public BooleanTypeConvert(BooleanValueContainer container) {
            this.container = container;
        }

        @Override
        public void addBoolean(boolean value) {
            container.addBoolean(value);
        }
    }

}
