// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import * as arrow from "../lancedb/arrow";
import {
  sanitizeField,
  sanitizeMap,
  sanitizeSchema,
  sanitizeType,
} from "../lancedb/sanitize";
import { arrow21 } from "./arrow_versions";

describe("sanitize", function () {
  describe("sanitizeType function", function () {
    it("should handle type objects", function () {
      const type = new arrow.Int32();
      const result = sanitizeType(type);

      expect(result.typeId).toBe(arrow.Type.Int);
      expect((result as arrow.Int).bitWidth).toBe(32);
      expect((result as arrow.Int).isSigned).toBe(true);

      const floatType = {
        typeId: 3, // Type.Float = 3
        precision: 2,
        toString: () => "Float",
        isFloat: true,
        isFixedWidth: true,
      };

      const floatResult = sanitizeType(floatType);
      expect(floatResult).toBeInstanceOf(arrow.DataType);
      expect(floatResult.typeId).toBe(arrow.Type.Float);

      const floatResult2 = sanitizeType({ ...floatType, typeId: () => 3 });
      expect(floatResult2).toBeInstanceOf(arrow.DataType);
      expect(floatResult2.typeId).toBe(arrow.Type.Float);
    });

    const allTypeNameTestCases = [
      ["null", new arrow.Null()],
      ["binary", new arrow.Binary()],
      ["utf8", new arrow.Utf8()],
      ["bool", new arrow.Bool()],
      ["int8", new arrow.Int8()],
      ["int16", new arrow.Int16()],
      ["int32", new arrow.Int32()],
      ["int64", new arrow.Int64()],
      ["uint8", new arrow.Uint8()],
      ["uint16", new arrow.Uint16()],
      ["uint32", new arrow.Uint32()],
      ["uint64", new arrow.Uint64()],
      ["float16", new arrow.Float16()],
      ["float32", new arrow.Float32()],
      ["float64", new arrow.Float64()],
      ["datemillisecond", new arrow.DateMillisecond()],
      ["dateday", new arrow.DateDay()],
      ["timenanosecond", new arrow.TimeNanosecond()],
      ["timemicrosecond", new arrow.TimeMicrosecond()],
      ["timemillisecond", new arrow.TimeMillisecond()],
      ["timesecond", new arrow.TimeSecond()],
      ["intervaldaytime", new arrow.IntervalDayTime()],
      ["intervalyearmonth", new arrow.IntervalYearMonth()],
      ["durationnanosecond", new arrow.DurationNanosecond()],
      ["durationmicrosecond", new arrow.DurationMicrosecond()],
      ["durationmillisecond", new arrow.DurationMillisecond()],
      ["durationsecond", new arrow.DurationSecond()],
      ["largebinary", new arrow.LargeBinary()],
      ["largeutf8", new arrow.LargeUtf8()],
    ] as const;

    it.each(allTypeNameTestCases)(
      'should map type name "%s" to %s',
      function (name, expected) {
        const result = sanitizeType(name);
        expect(result).toBeInstanceOf(expected.constructor);
      },
    );

    const caseVariationTestCases = [
      ["NULL", new arrow.Null()],
      ["Utf8", new arrow.Utf8()],
      ["FLOAT32", new arrow.Float32()],
      ["DaTedAy", new arrow.DateDay()],
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

    // These types only exist in Arrow 20 (IntervalMonthDayNano) and 21 (the
    // rest), so the objects come from a second copy of the newest release.  The
    // library itself may be running on an older Arrow (see the CI matrix), in
    // which case sanitizing them has to fail with a clear message instead.
    const hostArrow = arrow as unknown as Record<string, unknown>;
    const hostSupportsNewerTypes = typeof hostArrow.Utf8View === "function";
    const whenSupported = hostSupportsNewerTypes ? describe : describe.skip;
    const whenUnsupported = hostSupportsNewerTypes ? describe.skip : describe;

    const newerTypeTestCases = [
      ["Utf8View", () => new arrow21.Utf8View()],
      ["BinaryView", () => new arrow21.BinaryView()],
      ["IntervalMonthDayNano", () => new arrow21.IntervalMonthDayNano()],
      [
        "LargeList",
        () =>
          new arrow21.LargeList(new arrow21.Field("item", new arrow21.Utf8())),
      ],
    ] as const;

    whenSupported("types added in Arrow 20 and 21", function () {
      it.each(newerTypeTestCases)(
        "should sanitize a %s type from another copy of Arrow",
        function (name, makeForeign) {
          const foreign = makeForeign();
          const result = sanitizeType(foreign);
          // Interval subclasses share `Type.Interval`, so the sanitized type
          // is the generic `Interval` carrying the same unit, as for the
          // other units.
          const expected = name === "IntervalMonthDayNano" ? "Interval" : name;
          expect(result).toBeInstanceOf(hostArrow[expected] as () => unknown);
          expect(result.typeId).toBe(foreign.typeId);
          expect(result.toString()).toBe(foreign.toString());
        },
      );

      it.each([
        ["binaryview", "BinaryView"],
        ["utf8view", "Utf8View"],
        ["intervalmonthdaynano", "Interval"],
      ])('should map type name "%s" to %s', function (name, expected) {
        expect(sanitizeType(name)).toBeInstanceOf(
          hostArrow[expected] as () => unknown,
        );
      });

      it("should sanitize the child field of a LargeList", function () {
        const foreign = new arrow21.LargeList(
          new arrow21.Field("item", new arrow21.Int32(), false),
        );
        const result = sanitizeType(foreign) as arrow.List;
        expect(result.children[0]).toBeInstanceOf(arrow.Field);
        expect(result.children[0].name).toBe("item");
        expect(result.children[0].nullable).toBe(false);
        const child = result.children[0].type;
        expect(arrow.DataType.isInt(child)).toBe(true);
        expect((child as arrow.Int).bitWidth).toBe(32);
        expect((child as arrow.Int).isSigned).toBe(true);
      });
    });

    whenUnsupported("types newer than the installed Arrow", function () {
      it.each(
        newerTypeTestCases.filter(([name]) => name !== "IntervalMonthDayNano"),
      )(
        "should explain that %s needs a newer apache-arrow",
        function (name, makeForeign) {
          expect(() => sanitizeType(makeForeign())).toThrow(
            `The Arrow type ${name} requires apache-arrow`,
          );
        },
      );

      it("should sanitize IntervalMonthDayNano to a generic Interval", () => {
        // The subclass shares `Type.Interval`, which every supported Arrow
        // release has; only the unit is new.
        const result = sanitizeType(new arrow21.IntervalMonthDayNano());
        expect(result).toBeInstanceOf(arrow.Interval);
        expect((result as arrow.Interval).unit).toBe(
          arrow21.IntervalUnit.MONTH_DAY_NANO,
        );
      });
    });

    it("should rebuild a schema from a second copy of the same Arrow version", function () {
      // Arrow 21 makes `instanceof Schema` succeed across copies of the library
      // but not `instanceof <DataType>`; the schema must still be rebuilt.
      const foreign = new arrow21.Schema([
        new arrow21.Field("x", new arrow21.Float32(), true),
      ]);
      // Only Arrow 21+ overrides `Symbol.hasInstance` this way.
      const crossCopyInstanceof = Object.getOwnPropertySymbols(
        arrow.Schema,
      ).includes(Symbol.hasInstance);
      expect(foreign instanceof arrow.Schema).toBe(crossCopyInstanceof);
      const result = sanitizeSchema(foreign);
      expect(result).not.toBe(foreign);
      const type = result.fields[0].type;
      expect(type).toBeInstanceOf(arrow.DataType);
      expect(arrow.DataType.isFloat(type)).toBe(true);
      expect((type as arrow.Float).precision).toBe(arrow.Precision.SINGLE);
      expect(() => arrow.makeBuilder({ type })).not.toThrow();
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

      expect(field).toBeInstanceOf(arrow.Field);
      expect(field.name).toBe("string_field");
      expect(field.type).toBeInstanceOf(arrow.Utf8);
      expect(field.nullable).toBe(true);
      expect(field.metadata?.get("key")).toBe("value");
    });

    it("should handle field with type object", function () {
      const floatType = {
        typeId: 3, // Float
        precision: 32,
      };

      const field = sanitizeField({
        name: "float_field",
        type: floatType,
        nullable: false,
      });

      expect(field).toBeInstanceOf(arrow.Field);
      expect(field.name).toBe("float_field");
      expect(field.type).toBeInstanceOf(arrow.DataType);
      expect(field.type.typeId).toBe(arrow.Type.Float);
      expect((field.type as arrow.Float64).precision).toBe(32);
      expect(field.nullable).toBe(false);
    });

    it("should handle field with direct Type instance", function () {
      const field = sanitizeField({
        name: "bool_field",
        type: new arrow.Bool(),
        nullable: true,
      });

      expect(field).toBeInstanceOf(arrow.Field);
      expect(field.name).toBe("bool_field");
      expect(field.type).toBeInstanceOf(arrow.Bool);
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

  describe("sanitizeMap function", function () {
    it.each([
      ["no children", []],
      ["two children", [{}, {}]],
    ])("should reject a Map type with %s", function (_, children) {
      expect(() => sanitizeMap({ children, keysSorted: false })).toThrow(
        "Expected a Map type to have exactly one child",
      );
    });
  });
});
