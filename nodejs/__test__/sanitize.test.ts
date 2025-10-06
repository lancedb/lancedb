import { DataType, Field, Int, Type } from "../lancedb/arrow";
import {
  Binary,
  Bool,
  DateDay,
  DateMillisecond,
  DurationMicrosecond,
  DurationMillisecond,
  DurationNanosecond,
  DurationSecond,
  Float16,
  Float32,
  Float64,
  Int8,
  Int16,
  Int32,
  Int64,
  IntervalDayTime,
  IntervalYearMonth,
  Null,
  TimeMicrosecond,
  TimeMillisecond,
  TimeNanosecond,
  TimeSecond,
  Uint8,
  Uint16,
  Uint32,
  Uint64,
  Utf8,
} from "../lancedb/arrow";
import { sanitizeField, sanitizeType } from "../lancedb/sanitize";

describe("sanitize", function () {
  describe("sanitizeType function", function () {
    it("should handle type objects", function () {
      const type = new Int32();
      const result = sanitizeType(type);

      expect(result.typeId).toBe(Type.Int);
      expect((result as Int).bitWidth).toBe(32);
      expect((result as Int).isSigned).toBe(true);

      const floatType = {
        typeId: 3, // Type.Float = 3
        precision: 2,
        toString: () => "Float",
        isFloat: true,
        isFixedWidth: true,
      };

      const floatResult = sanitizeType(floatType);
      expect(floatResult).toBeInstanceOf(DataType);
      expect(floatResult.typeId).toBe(Type.Float);

      const floatResult2 = sanitizeType({ ...floatType, typeId: () => 3 });
      expect(floatResult2).toBeInstanceOf(DataType);
      expect(floatResult2.typeId).toBe(Type.Float);
    });

    const allTypeNameTestCases = [
      ["null", new Null()],
      ["binary", new Binary()],
      ["utf8", new Utf8()],
      ["bool", new Bool()],
      ["int8", new Int8()],
      ["int16", new Int16()],
      ["int32", new Int32()],
      ["int64", new Int64()],
      ["uint8", new Uint8()],
      ["uint16", new Uint16()],
      ["uint32", new Uint32()],
      ["uint64", new Uint64()],
      ["float16", new Float16()],
      ["float32", new Float32()],
      ["float64", new Float64()],
      ["datemillisecond", new DateMillisecond()],
      ["dateday", new DateDay()],
      ["timenanosecond", new TimeNanosecond()],
      ["timemicrosecond", new TimeMicrosecond()],
      ["timemillisecond", new TimeMillisecond()],
      ["timesecond", new TimeSecond()],
      ["intervaldaytime", new IntervalDayTime()],
      ["intervalyearmonth", new IntervalYearMonth()],
      ["durationnanosecond", new DurationNanosecond()],
      ["durationmicrosecond", new DurationMicrosecond()],
      ["durationmillisecond", new DurationMillisecond()],
      ["durationsecond", new DurationSecond()],
    ] as const;

    it.each(allTypeNameTestCases)(
      'should map type name "%s" to %s',
      function (name, expected) {
        const result = sanitizeType(name);
        expect(result).toBeInstanceOf(expected.constructor);
      },
    );

    const caseVariationTestCases = [
      ["NULL", new Null()],
      ["Utf8", new Utf8()],
      ["FLOAT32", new Float32()],
      ["DaTedAy", new DateDay()],
    ] as const;

    it.each(caseVariationTestCases)(
      'should be case insensitive for type name "%s" mapped to %s',
      function (name, expected) {
        const result = sanitizeType(name);
        expect(result).toBeInstanceOf(expected.constructor);
      },
    );

    it("should throw error for unrecognized type name", function () {
      expect(() => sanitizeType("invalid_type")).toThrow(
        "Unrecognized type name in schema: invalid_type",
      );
    });
  });

  describe("sanitizeField function", function () {
    it("should handle field with string type name", function () {
      const field = sanitizeField({
        name: "string_field",
        type: "utf8",
        nullable: true,
        metadata: new Map([["key", "value"]]),
      });

      expect(field).toBeInstanceOf(Field);
      expect(field.name).toBe("string_field");
      expect(field.type).toBeInstanceOf(Utf8);
      expect(field.nullable).toBe(true);
      expect(field.metadata?.get("key")).toBe("value");
    });

    it("should handle field with type object", function () {
      const floatType = {
        typeId: 3, // Float
        precision: 2,
      };

      const field = sanitizeField({
        name: "float_field",
        type: floatType,
        nullable: false,
      });

      expect(field).toBeInstanceOf(Field);
      expect(field.name).toBe("float_field");
      expect(field.type).toBeInstanceOf(DataType);
      expect(field.nullable).toBe(false);
    });

    it("should handle field with direct Type instance", function () {
      const field = sanitizeField({
        name: "bool_field",
        type: new Bool(),
        nullable: true,
      });

      expect(field).toBeInstanceOf(Field);
      expect(field.name).toBe("bool_field");
      expect(field.type).toBeInstanceOf(Bool);
      expect(field.nullable).toBe(true);
    });

    it("should throw error for invalid field object", function () {
      expect(() =>
        sanitizeField({
          type: "int32",
          nullable: true,
        }),
      ).toThrow(
        "The field passed in is missing a `type`/`name`/`nullable` property",
      );

      // Invalid type
      expect(() =>
        sanitizeField({
          name: "invalid",
          type: { invalid: true },
          nullable: true,
        }),
      ).toThrow("Expected a Type to have a typeId property");

      // Invalid nullable
      expect(() =>
        sanitizeField({
          name: "invalid_nullable",
          type: "int32",
          nullable: "not a boolean",
        }),
      ).toThrow("The field passed in had a non-boolean `nullable` property");
    });

    it("should report error for invalid type name", function () {
      expect(() =>
        sanitizeField({
          name: "invalid_field",
          type: "invalid_type",
          nullable: true,
        }),
      ).toThrow(
        "Unable to sanitize type for field: invalid_field due to error: Error: Unrecognized type name in schema: invalid_type",
      );
    });
  });
});
