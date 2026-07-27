// @vitest-environment happy-dom
import assert from "node:assert/strict";
import { test } from "vitest";

import { highlightCodeBlocks } from "./syntax-highlighting";

const SUPPORTED_LANGUAGES = [
  "bash",
  "c",
  "cpp",
  "csharp",
  "css",
  "diff",
  "go",
  "java",
  "javascript",
  "json",
  "kotlin",
  "markdown",
  "python",
  "ruby",
  "rust",
  "sql",
  "typescript",
  "xml",
  "yaml",
];

test("highlights every explicitly supported language", () => {
  const root = document.createElement("div");
  const codeBlocks = SUPPORTED_LANGUAGES.map((language) => {
    const pre = document.createElement("pre");
    const code = document.createElement("code");
    code.className = `language-${language}`;
    code.textContent = "example";
    pre.append(code);
    root.append(pre);
    return code;
  });

  highlightCodeBlocks(root);

  for (const [index, code] of codeBlocks.entries()) {
    assert.equal(
      code.dataset.highlighted,
      "yes",
      `expected ${SUPPORTED_LANGUAGES[index]} to be highlighted`,
    );
  }
});
