package com.lancedb.lancedb;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.*;

/** Utility class for converting between Java objects and Arrow format. */
public class DataConverter {
    private static final BufferAllocator allocator = new RootAllocator();

    /**
     * Convert a list of maps to Arrow VectorSchemaRoot.
     *
     * @param data List of maps representing records
     * @return Arrow VectorSchemaRoot
     */
    public static VectorSchemaRoot convertToArrow(List<Map<String, Object>> data) {
        if (data.isEmpty()) {
            throw new IllegalArgumentException("Data cannot be empty");
        }

        // Infer schema from first record
        Map<String, Object> firstRecord = data.get(0);
        List<Field> fields = new ArrayList<>();

        for (Map.Entry<String, Object> entry : firstRecord.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue();
            Field field = inferField(fieldName, value);
            fields.add(field);
        }

        Schema schema = new Schema(fields);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();

        // Populate data
        for (int i = 0; i < data.size(); i++) {
            Map<String, Object> record = data.get(i);
            for (int j = 0; j < fields.size(); j++) {
                Field field = fields.get(j);
                Object value = record.get(field.getName());
                setVectorValue(root.getVector(j), i, value);
            }
        }

        root.setRowCount(data.size());
        return root;
    }

    /**
     * Convert Arrow VectorSchemaRoot to list of maps.
     *
     * @param root Arrow VectorSchemaRoot
     * @return List of maps representing records
     */
    public static List<Map<String, Object>> convertFromArrow(VectorSchemaRoot root) {
        List<Map<String, Object>> result = new ArrayList<>();
        int rowCount = root.getRowCount();

        for (int i = 0; i < rowCount; i++) {
            Map<String, Object> record = new HashMap<>();
            for (FieldVector vector : root.getFieldVectors()) {
                String fieldName = vector.getField().getName();
                Object value = getVectorValue(vector, i);
                record.put(fieldName, value);
            }
            result.add(record);
        }

        return result;
    }

    private static Field inferField(String name, Object value) {
        if (value instanceof Integer) {
            return Field.nullable(name, new ArrowType.Int(32, true));
        } else if (value instanceof Long) {
            return Field.nullable(name, new ArrowType.Int(64, true));
        } else if (value instanceof Float) {
            return Field.nullable(name, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        } else if (value instanceof Double) {
            return Field.nullable(name, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
        } else if (value instanceof String) {
            return Field.nullable(name, new ArrowType.Utf8());
        } else if (value instanceof Boolean) {
            return Field.nullable(name, new ArrowType.Bool());
        } else if (value instanceof List) {
            // Assume float vector for now
            return Field.nullable(name, new ArrowType.List());
        } else {
            throw new IllegalArgumentException("Unsupported data type: " + value.getClass());
        }
    }

    private static void setVectorValue(FieldVector vector, int index, Object value) {
        if (value == null) {
            vector.setNull(index);
            return;
        }

        if (vector instanceof IntVector) {
            ((IntVector) vector).set(index, (Integer) value);
        } else if (vector instanceof BigIntVector) {
            ((BigIntVector) vector).set(index, (Long) value);
        } else if (vector instanceof Float4Vector) {
            ((Float4Vector) vector).set(index, (Float) value);
        } else if (vector instanceof Float8Vector) {
            ((Float8Vector) vector).set(index, (Double) value);
        } else if (vector instanceof VarCharVector) {
            ((VarCharVector) vector).setSafe(index, ((String) value).getBytes());
        } else if (vector instanceof BitVector) {
            ((BitVector) vector).set(index, (Boolean) value ? 1 : 0);
        } else if (vector instanceof ListVector) {
            ListVector listVector = (ListVector) vector;
            @SuppressWarnings("unchecked")
            List<Float> list = (List<Float>) value;
            listVector.startNewValue(index);
            for (int i = 0; i < list.size(); i++) {
                ((Float4Vector) listVector.getDataVector()).setSafe(listVector.getOffsetBuffer().getInt(index * 4) + i, list.get(i));
            }
            listVector.endValue(index, list.size());
        }
    }

    private static Object getVectorValue(FieldVector vector, int index) {
        if (vector.isNull(index)) {
            return null;
        }

        if (vector instanceof IntVector) {
            return ((IntVector) vector).get(index);
        } else if (vector instanceof BigIntVector) {
            return ((BigIntVector) vector).get(index);
        } else if (vector instanceof Float4Vector) {
            return ((Float4Vector) vector).get(index);
        } else if (vector instanceof Float8Vector) {
            return ((Float8Vector) vector).get(index);
        } else if (vector instanceof VarCharVector) {
            return new String(((VarCharVector) vector).get(index));
        } else if (vector instanceof BitVector) {
            return ((BitVector) vector).get(index) == 1;
        } else if (vector instanceof ListVector) {
            ListVector listVector = (ListVector) vector;
            int start = listVector.getElementStartIndex(index);
            int end = listVector.getElementEndIndex(index);
            List<Float> result = new ArrayList<>();
            Float4Vector dataVector = (Float4Vector) listVector.getDataVector();
            for (int i = start; i < end; i++) {
                result.add(dataVector.get(i));
            }
            return result;
        }

        return null;
    }
}