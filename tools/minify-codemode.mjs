import { join } from "node:path";

// Mark the expression as an export so Bun keeps the function during tree shaking.
// QuickJS evaluates the resulting expression and receives the function itself.
const transpiler = new Bun.Transpiler({ loader: "js", minifyWhitespace: true });
const prefix = "export default ";
for (const file of ["bootstrap.js", "decoder.js"]) {
  const source = await Bun.file(join("src", "codemode", file)).text();
  const output = transpiler.transformSync(prefix + source);
  if (!output.startsWith(prefix)) throw new Error("unexpected Bun output for " + file);
  await Bun.write(join(process.argv[2], file), output.slice(prefix.length));
}
