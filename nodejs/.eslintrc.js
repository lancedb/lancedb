module.exports = {
  env: {
    browser: true,
    es2021: true,
  },
  extends: [
    "eslint:recommended",
    "plugin:@typescript-eslint/recommended-type-checked",
    "plugin:@typescript-eslint/stylistic-type-checked",
  ],
  overrides: [],
  parserOptions: {
    project: "./tsconfig.json",
    ecmaVersion: "latest",
    sourceType: "module",
  },
  rules: {
    "@typescript-eslint/method-signature-style": "off",
    "@typescript-eslint/no-explicit-any": "off",
  },
  ignorePatterns: ["node_modules/", "dist/", "build/", "vectordb/native.*"],
};
