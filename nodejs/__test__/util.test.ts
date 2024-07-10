import { IntoSql, toSQL } from "../lancedb/util";
test.each([
  ["string", "'string'"],
  [123, "123"],
  [1.11, "1.11"],
  [true, "TRUE"],
  [false, "FALSE"],
  [null, "NULL"],
  [new Date("2021-01-01T00:00:00.000Z"), "'2021-01-01T00:00:00.000Z'"],
  [[1, 2, 3], "[1, 2, 3]"],
  [new ArrayBuffer(8), "X'0000000000000000'"],
  [Buffer.from("hello"), "X'68656c6c6f'"],
  ["Hello 'world'", "'Hello ''world'''"],
])("toSQL(%p) === %p", (value, expected) => {
  expect(toSQL(value)).toBe(expected);
});

test("toSQL({}) throws on unsupported value type", () => {
  expect(() => toSQL({} as unknown as IntoSql)).toThrow(
    "Unsupported value type: object value: ([object Object])",
  );
});
test("toSQL() throws on unsupported value type", () => {
  // biome-ignore lint/suspicious/noExplicitAny: <explanation>
  expect(() => (<any>toSQL)()).toThrow(
    "Unsupported value type: undefined value: (undefined)",
  );
});
