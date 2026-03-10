#!/usr/bin/env python3

from __future__ import annotations

import subprocess
from pathlib import Path

import markdown


ROOT = Path(__file__).resolve().parents[1]
BUILD_DIR = ROOT / ".build"
HTML_OUT = BUILD_DIR / "Crawfish-PRD.export.html"
DOCX_OUT = ROOT / "docs" / "exports" / "Crawfish-PRD.docx"

DOC_ORDER = [
    ROOT / "README.md",
    ROOT / "docs" / "spec" / "philosophy.md",
    ROOT / "docs" / "spec" / "vision.md",
    ROOT / "docs" / "spec" / "architecture.md",
    ROOT / "docs" / "spec" / "v0.1-plan.md",
    ROOT / "docs" / "spec" / "glossary.md",
]

HTML_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Crawfish Documentation Set</title>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
      color: #1f2933;
      line-height: 1.55;
      margin: 48px;
      max-width: 920px;
    }}
    h1, h2, h3, h4 {{
      color: #123b5d;
      margin-top: 1.4em;
      margin-bottom: 0.45em;
    }}
    h1 {{
      font-size: 32px;
      border-bottom: 3px solid #2d6a9f;
      padding-bottom: 8px;
    }}
    h2 {{
      font-size: 22px;
      border-bottom: 1px solid #d6e4ef;
      padding-bottom: 4px;
    }}
    h3 {{
      font-size: 17px;
    }}
    table {{
      border-collapse: collapse;
      width: 100%;
      margin: 14px 0 22px 0;
      font-size: 13px;
    }}
    th, td {{
      border: 1px solid #d6e4ef;
      padding: 8px 10px;
      vertical-align: top;
    }}
    th {{
      background: #f2f7fb;
      color: #123b5d;
      text-align: left;
    }}
    pre {{
      white-space: pre-wrap;
      font-family: Menlo, Consolas, monospace;
      font-size: 12px;
      background: #0f1720;
      color: #e5edf5;
      padding: 14px;
      border-radius: 6px;
      margin: 12px 0 20px 0;
    }}
    code {{
      font-family: Menlo, Consolas, monospace;
    }}
    hr {{
      border: none;
      border-top: 1px solid #d6e4ef;
      margin: 36px 0;
    }}
    blockquote {{
      border-left: 4px solid #7aa6c8;
      padding: 8px 14px;
      margin: 14px 0;
      color: #334e68;
      background: #f7fbfe;
    }}
  </style>
</head>
<body>
{body}
</body>
</html>
"""


def render_markdown(path: Path) -> str:
    text = path.read_text(encoding="utf-8")
    return markdown.markdown(
        text,
        extensions=[
            "fenced_code",
            "tables",
            "sane_lists",
            "toc",
        ],
        output_format="html5",
    )


def main() -> None:
    BUILD_DIR.mkdir(parents=True, exist_ok=True)
    DOCX_OUT.parent.mkdir(parents=True, exist_ok=True)
    sections = []
    for path in DOC_ORDER:
        sections.append(render_markdown(path))
    body = "\n<hr/>\n".join(sections)
    html = HTML_TEMPLATE.format(body=body)
    HTML_OUT.write_text(html, encoding="utf-8")

    subprocess.run(
        ["textutil", "-convert", "docx", str(HTML_OUT), "-output", str(DOCX_OUT)],
        check=True,
    )
    print(DOCX_OUT)


if __name__ == "__main__":
    main()
