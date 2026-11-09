import tseslint from "typescript-eslint";
export default tseslint.config(
  { ignores: ["node_modules/**", ".alchemy/**", ".wrangler/**"] },
  ...tseslint.configs.recommended,
  { rules: { "@typescript-eslint/no-unused-vars": ["error", { argsIgnorePattern: "^_" }] } },
);
