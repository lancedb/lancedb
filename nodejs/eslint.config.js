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
      "@typescript-eslint/naming-convention": "error",
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
