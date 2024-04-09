/* eslint-disable @typescript-eslint/naming-convention */
// @ts-check

const eslint = require("@eslint/js");
const tseslint = require("typescript-eslint");
const eslintConfigPrettier = require("eslint-config-prettier");
const jsdoc = require("eslint-plugin-jsdoc");

module.exports = tseslint.config(
  eslint.configs.recommended,
  jsdoc.configs["flat/recommended"],
  eslintConfigPrettier,
  ...tseslint.configs.recommended,
  {
    rules: {
      "@typescript-eslint/naming-convention": [
        "error",
        // Make exceptions for AWS SDK properties
        {
          selector: "property",
          format: null,
          filter: {
            regex: "(Bucket|Key|KeyId|Prefix)",
            match: true,
          },
        },
        // Defaults from https://typescript-eslint.io/rules/naming-convention/#options
        {
          selector: "default",
          format: ["camelCase"],
          leadingUnderscore: "allow",
          trailingUnderscore: "allow",
        },
        {
          selector: "import",
          format: ["camelCase", "PascalCase"],
        },
        {
          selector: "variable",
          format: ["camelCase", "UPPER_CASE"],
          leadingUnderscore: "allow",
          trailingUnderscore: "allow",
        },
        {
          selector: "typeLike",
          format: ["PascalCase"],
        },
      ],
      "jsdoc/require-returns": "off",
      "jsdoc/require-param": "off",
      "jsdoc/require-jsdoc": [
        "error",
        {
          publicOnly: true,
        },
      ],
    },
    plugins: jsdoc,
  },
);
