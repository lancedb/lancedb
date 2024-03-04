module.exports = {
  env: {
    browser: true,
    es2021: true
  },
  extends: 'standard-with-typescript',
  overrides: [
  ],
  parserOptions: {
    project: './tsconfig.json',
    ecmaVersion: 'latest',
    sourceType: 'module'
  },
  rules: {
    "@typescript-eslint/method-signature-style": "off",
    "@typescript-eslint/quotes": "off",
    "@typescript-eslint/semi": "off",
    "@typescript-eslint/explicit-function-return-type": "off",
    "@typescript-eslint/space-before-function-paren": "off",
    "@typescript-eslint/indent": "off",
  }
}
