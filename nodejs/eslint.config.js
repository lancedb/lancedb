// @ts-check

const eslint = require("@eslint/js");
const tseslint = require("typescript-eslint");
const eslintConfigPrettier = require("eslint-config-prettier");

module.exports = tseslint.config(
  eslint.configs.recommended,
  eslintConfigPrettier,
  ...tseslint.configs.recommended,
);
