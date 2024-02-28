package com.sdu.data.hbase;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class HPutParam {

    private final String tableName;
    private final String rowKey;

    private final List<PutColumnParam> columnPutParams;

    private HPutParam(String tableName, String rowKey, List<PutColumnParam> columnPutParams) {
        this.tableName = tableName;
        this.rowKey = rowKey;
        this.columnPutParams = columnPutParams;
    }

    public String getTableName() {
        return tableName;
    }

    public String getRowKey() {
        return rowKey;
    }

    public List<PutColumnParam> getColumnPutParams() {
        return columnPutParams;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class PutColumnParam {
        private final String family;
        private final Map<String, byte[]> qualifierValues;

        private PutColumnParam(String family, Map<String, byte[]> qualifierValues) {
            checkArgument(family != null && !family.isEmpty());
            checkArgument(qualifierValues != null && !qualifierValues.isEmpty());
            this.family = family;
            this.qualifierValues = qualifierValues;
        }

        public String getFamily() {
            return family;
        }

        public Map<String, byte[]> getQualifierValues() {
            return qualifierValues;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private String family;
            private Map<String, byte[]> qualifierValues;

            public Builder setFamily(String family) {
                checkArgument(family != null && !family.isEmpty());
                this.family = family;
                return this;
            }

            public Builder addQualifier(String qualifier, byte[] value) {
                checkArgument(qualifier != null && !qualifier.isEmpty());
                checkArgument(value != null && value.length != 0);
                if (qualifierValues == null) {
                    qualifierValues = new HashMap<>();
                }
                qualifierValues.put(qualifier, value);
                return this;
            }

            public PutColumnParam build() {
                return new PutColumnParam(family, qualifierValues);
            }
        }

    }

    public static class Builder {
        private String tableName;
        private String rowKey;
        private List<PutColumnParam> columnPutParams;

        public Builder setTableName(String tableName) {
            checkArgument(tableName != null && !tableName.isEmpty());
            this.tableName = tableName;
            return this;
        }

        public Builder setRowKey(String rowKey) {
            checkArgument(rowKey != null && !rowKey.isEmpty());
            this.rowKey = rowKey;
            return this;
        }

        public Builder addPutColumnParam(PutColumnParam param) {
            checkArgument(param != null);
            if (columnPutParams == null) {
                columnPutParams = new LinkedList<>();
            }
            columnPutParams.add(param);
            return this;
        }

        public HPutParam build() {
            return new HPutParam(tableName, rowKey, columnPutParams);
        }
    }
}
