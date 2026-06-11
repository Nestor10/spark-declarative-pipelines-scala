package dev.sdp.core.algebra

/** Parser for the Spark DDL schema-string dialect — the same string
  * `DataFrameReader.schema(...)` / `DataStreamReader.schema(...)` accept:
  *
  * {{{
  * "value BIGINT, seen_at TIMESTAMP, `weird name` STRING"
  * }}}
  *
  * Pure function (Domain Core): the DSL macro calls it at compile time so a
  * malformed string is a *positioned compile error*, and `sdpImportSchemas` /
  * tests can exercise it without a macro in sight.
  *
  * Type mapping is **gradual** (principle: gradual or bust): the checked
  * column types map exactly; any other syntactically valid Spark type
  * (`DECIMAL(10,2)`, `ARRAY<INT>`, `STRUCT<...>`, ...) keeps the column name
  * — so name-level schema checking still sees it — with `ColType.Unknown`.
  * A trailing `NOT NULL` is accepted and ignored (nullability is the
  * server's concern).
  */
object DdlSchema:

  /** Parse a DDL schema string into declared columns. Left = human-readable
    * error (the macro positions it at the string literal).
    */
  def parse(ddl: String): Either[String, List[(String, ColType)]] =
    if ddl.trim.isEmpty then Left("schema DDL is empty")
    else
      splitTopLevel(ddl).flatMap { fields =>
        fields.foldRight(Right(Nil): Either[String, List[(String, ColType)]]) { (f, acc) =>
          for { rest <- acc; col <- parseField(f) } yield col :: rest
        }
      }

  /** Render checked columns back to a DDL string (the inverse of [[parse]] for
    * modelled types) — used by the `withSchema(field(...))` path, whose tokens
    * are all checked types. Names with non-identifier chars are backtick-quoted.
    * Note: for the `.schema("...")` path we keep the *raw* DDL instead, so types
    * `ColType` can't model (DECIMAL, ARRAY, …) survive verbatim. */
  def render(cols: List[(String, ColType)]): String =
    cols.map((n, t) => s"${quoteName(n)} ${ddlKeyword(t)}").mkString(", ")

  private def quoteName(n: String): String =
    if n.nonEmpty && n.forall(c => c.isLetterOrDigit || c == '_') then n
    else s"`${n.replace("`", "``")}`"

  private def ddlKeyword(t: ColType): String = t match
    case ColType.Bool      => "BOOLEAN"
    case ColType.I32       => "INT"
    case ColType.I64       => "BIGINT"
    case ColType.F64       => "DOUBLE"
    case ColType.Str       => "STRING"
    case ColType.Timestamp => "TIMESTAMP"
    case ColType.Date      => "DATE"
    case ColType.Unknown   => "STRING" // unreachable from withSchema tokens; defensive

  /** The checked types, with Spark's aliases. Everything else is Unknown. */
  private def typeOf(raw: String): ColType =
    raw.trim.toUpperCase match
      case "BOOLEAN"          => ColType.Bool
      case "INT" | "INTEGER"  => ColType.I32
      case "BIGINT" | "LONG"  => ColType.I64
      case "DOUBLE"           => ColType.F64
      case "STRING"           => ColType.Str
      case "TIMESTAMP"        => ColType.Timestamp
      case "DATE"             => ColType.Date
      case _                  => ColType.Unknown

  /** One `name TYPE [NOT NULL]` entry; the name may be backtick-quoted
    * (doubled backticks escape a literal one, as in Spark).
    */
  private def parseField(field: String): Either[String, (String, ColType)] =
    val trimmed = field.trim
    if trimmed.isEmpty then Left("empty column entry (stray comma?)")
    else if trimmed.startsWith("`") then
      // scan for the closing backtick, honouring `` escapes
      val sb = new StringBuilder
      var i  = 1
      var closed = -1
      while closed < 0 && i < trimmed.length do
        if trimmed.charAt(i) == '`' then
          if i + 1 < trimmed.length && trimmed.charAt(i + 1) == '`' then
            sb.append('`'); i += 2
          else closed = i
        else
          sb.append(trimmed.charAt(i)); i += 1
      if closed < 0 then Left(s"unterminated backtick-quoted name in: $trimmed")
      else typed(sb.toString, trimmed.substring(closed + 1), trimmed)
    else
      trimmed.split("\\s+", 2) match
        case Array(name, rest) => typed(name, rest, trimmed)
        case _                 => Left(s"missing type for column: $trimmed")

  private def typed(name: String, rawType: String, entry: String): Either[String, (String, ColType)] =
    val noNull = rawType.trim.replaceAll("(?i)\\s+NOT\\s+NULL\\s*$", "")
    if name.isEmpty then Left(s"empty column name in: $entry")
    else if noNull.isEmpty then Left(s"missing type for column: $name")
    else Right(name -> typeOf(noNull))

  /** Split on commas at nesting depth zero — `DECIMAL(10,2)` and
    * `STRUCT<a: INT, b: STRING>` keep their inner commas.
    */
  private def splitTopLevel(s: String): Either[String, List[String]] =
    val out   = List.newBuilder[String]
    val cur   = new StringBuilder
    var depth = 0
    var inTick = false
    s.foreach { c =>
      c match
        case '`'                       => inTick = !inTick; cur.append(c)
        case '(' | '<' if !inTick      => depth += 1; cur.append(c)
        case ')' | '>' if !inTick      => depth -= 1; cur.append(c)
        case ',' if depth == 0 && !inTick => out += cur.toString; cur.clear()
        case _                         => cur.append(c)
    }
    out += cur.toString
    if depth != 0 then Left(s"unbalanced brackets in schema DDL: $s")
    else if inTick then Left(s"unterminated backtick in schema DDL: $s")
    else Right(out.result())
