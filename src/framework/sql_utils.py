"""SQL script handling.

`spark.sql` executes one statement at a time, so a .sql file has to be split. Splitting
naively on ";" corrupts real DDL, because a semicolon appears inside string literals
(`COMMENT 'batch | stream; both use Auto Loader'`) and inside comments - and a chunk
that merely starts with a comment is not a comment.

This splitter tracks string literals and both comment forms, so only a top-level
semicolon ends a statement.
"""

from __future__ import annotations

from typing import Dict, List


def split_sql_statements(script: str) -> List[str]:
    """Split a SQL script into executable statements.

    Semicolons inside single- or double-quoted literals, backquoted identifiers,
    `-- line comments` and `/* block comments */` are not statement separators.
    Statements consisting only of comments and whitespace are dropped.
    """
    statements: List[str] = []
    current: List[str] = []

    in_single = in_double = in_backquote = False
    in_line_comment = in_block_comment = False

    index = 0
    length = len(script)

    while index < length:
        char = script[index]
        nxt = script[index + 1] if index + 1 < length else ""

        # ---- inside a comment: consume until it closes ----
        if in_line_comment:
            current.append(char)
            if char == "\n":
                in_line_comment = False
            index += 1
            continue
        if in_block_comment:
            current.append(char)
            if char == "*" and nxt == "/":
                current.append(nxt)
                in_block_comment = False
                index += 2
                continue
            index += 1
            continue

        # ---- inside a quoted region: only the matching quote can close it ----
        if in_single or in_double or in_backquote:
            current.append(char)
            if in_single and char == "'":
                # '' is an escaped quote, not the end of the literal.
                if nxt == "'":
                    current.append(nxt)
                    index += 2
                    continue
                in_single = False
            elif in_double and char == '"':
                in_double = False
            elif in_backquote and char == "`":
                in_backquote = False
            index += 1
            continue

        # ---- outside quotes and comments ----
        if char == "-" and nxt == "-":
            in_line_comment = True
            current.append(char)
            index += 1
            continue
        if char == "/" and nxt == "*":
            in_block_comment = True
            current.append(char)
            index += 1
            continue
        if char == "'":
            in_single = True
            current.append(char)
            index += 1
            continue
        if char == '"':
            in_double = True
            current.append(char)
            index += 1
            continue
        if char == "`":
            in_backquote = True
            current.append(char)
            index += 1
            continue
        if char == ";":
            statements.append("".join(current))
            current = []
            index += 1
            continue

        current.append(char)
        index += 1

    statements.append("".join(current))
    return [s.strip() for s in statements if _has_executable_content(s)]


def _has_executable_content(statement: str) -> bool:
    """True when the chunk holds SQL, not only comments and whitespace."""
    return bool(strip_sql_comments(statement).strip())


def strip_sql_comments(statement: str) -> str:
    """Remove line and block comments, leaving quoted literals untouched."""
    out: List[str] = []
    in_single = in_double = in_backquote = False
    in_line_comment = in_block_comment = False

    index = 0
    length = len(statement)
    while index < length:
        char = statement[index]
        nxt = statement[index + 1] if index + 1 < length else ""

        if in_line_comment:
            if char == "\n":
                in_line_comment = False
                out.append(char)
            index += 1
            continue
        if in_block_comment:
            if char == "*" and nxt == "/":
                in_block_comment = False
                index += 2
                continue
            index += 1
            continue
        if in_single or in_double or in_backquote:
            out.append(char)
            if in_single and char == "'":
                if nxt == "'":
                    out.append(nxt)
                    index += 2
                    continue
                in_single = False
            elif in_double and char == '"':
                in_double = False
            elif in_backquote and char == "`":
                in_backquote = False
            index += 1
            continue

        if char == "-" and nxt == "-":
            in_line_comment = True
            index += 2
            continue
        if char == "/" and nxt == "*":
            in_block_comment = True
            index += 2
            continue
        if char == "'":
            in_single = True
        elif char == '"':
            in_double = True
        elif char == "`":
            in_backquote = True
        out.append(char)
        index += 1
    return "".join(out)


def render_placeholders(script: str, substitutions: Dict[str, str]) -> str:
    """Replace ${key} placeholders, then verify none are left.

    An unresolved placeholder reaching Spark produces a baffling parse error, so it is
    reported here naming the placeholder instead.
    """
    for key, value in substitutions.items():
        script = script.replace("${" + key + "}", str(value))
    if "${" in script:
        start = script.index("${")
        raise ValueError(
            f"unresolved placeholder near {script[start:start + 60]!r}. "
            f"Provided substitutions: {sorted(substitutions)}"
        )
    return script
