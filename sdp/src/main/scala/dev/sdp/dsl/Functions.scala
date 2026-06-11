package dev.sdp.dsl

import dev.sdp.core.algebra.*

/** Runtime mirror of `dev.sdp.dsl.functions` (Functions.scala): the full Spark
  * `functions` surface. Every entry lowers to `Ex.Fn(wireName, args)` exactly
  * as the macro's `FacadeFunctions` table does (FlowExtractor.scala lines
  * 70–86) — identity wire names except `mean` → "avg" and `pow` → "power"
  * (Spark's own authority rule).
  *
  * Argument lowering mirrors `FlowExtractor.fnArg` (lines 435–447): a plain
  * Scala `Int` argument becomes `Ex.Lit(LitValue.I32(_))`, a `String` becomes
  * `Ex.Lit(LitValue.Str(_))`, a real Scala lambda becomes `Ex.Lam(...)`. So the
  * runtime signatures take `Int`/`String`/`Column => Column` exactly where the
  * macro stubs (Functions.scala) do, and produce the identical `Ex.Fn` args.
  *
  * Lambda parameter-name caveat: the macro captures the author's source
  * parameter name into `LamVar`; at runtime the lambda value carries no name,
  * so HOF facade entries synthesize the conventional names ("x"; "acc"/"x" for
  * aggregate; "l"/"r" for zip_with). Fixtures must use those same names to stay
  * render-identical with the macro (documented in the report).
  */
object functions:

  // helpers mirroring FlowExtractor.fnArg literal lowering
  private def li(v: Int): Ex    = Ex.Lit(LitValue.I32(v))
  private def ls(v: String): Ex = Ex.Lit(LitValue.Str(v))
  private def lng(v: Long): Ex  = Ex.Lit(LitValue.I64(v))

  // ---- aggregates -----------------------------------------------------
  def count(e: Column): Column        = Column(Ex.Fn("count", List(e.ex)))
  def sum(e: Column): Column          = Column(Ex.Fn("sum", List(e.ex)))
  def avg(e: Column): Column          = Column(Ex.Fn("avg", List(e.ex)))
  def mean(e: Column): Column         = Column(Ex.Fn("avg", List(e.ex))) // wire: avg
  def min(e: Column): Column          = Column(Ex.Fn("min", List(e.ex)))
  def max(e: Column): Column          = Column(Ex.Fn("max", List(e.ex)))
  def first(e: Column): Column        = Column(Ex.Fn("first", List(e.ex)))
  def last(e: Column): Column         = Column(Ex.Fn("last", List(e.ex)))
  def collect_list(e: Column): Column = Column(Ex.Fn("collect_list", List(e.ex)))
  def collect_set(e: Column): Column  = Column(Ex.Fn("collect_set", List(e.ex)))

  // ---- window functions -----------------------------------------------
  def row_number(): Column = Column(Ex.Fn("row_number", Nil))
  def rank(): Column       = Column(Ex.Fn("rank", Nil))
  def dense_rank(): Column = Column(Ex.Fn("dense_rank", Nil))
  def ntile(n: Int): Column = Column(Ex.Fn("ntile", List(li(n))))
  def lag(e: Column, offset: Int): Column = Column(Ex.Fn("lag", List(e.ex, li(offset))))
  def lag(e: Column, offset: Int, default: Column): Column =
    Column(Ex.Fn("lag", List(e.ex, li(offset), default.ex)))
  def lead(e: Column, offset: Int): Column = Column(Ex.Fn("lead", List(e.ex, li(offset))))
  def lead(e: Column, offset: Int, default: Column): Column =
    Column(Ex.Fn("lead", List(e.ex, li(offset), default.ex)))

  // ---- strings --------------------------------------------------------
  def upper(e: Column): Column  = Column(Ex.Fn("upper", List(e.ex)))
  def lower(e: Column): Column  = Column(Ex.Fn("lower", List(e.ex)))
  def trim(e: Column): Column   = Column(Ex.Fn("trim", List(e.ex)))
  def length(e: Column): Column = Column(Ex.Fn("length", List(e.ex)))
  def concat(exprs: Column*): Column = Column(Ex.Fn("concat", exprs.map(_.ex).toList))
  def concat_ws(sep: String, exprs: Column*): Column =
    Column(Ex.Fn("concat_ws", ls(sep) :: exprs.map(_.ex).toList))
  def substring(e: Column, pos: Int, len: Int): Column =
    Column(Ex.Fn("substring", List(e.ex, li(pos), li(len))))
  def split(e: Column, pattern: String): Column =
    Column(Ex.Fn("split", List(e.ex, ls(pattern))))
  def regexp_replace(e: Column, pattern: String, replacement: String): Column =
    Column(Ex.Fn("regexp_replace", List(e.ex, ls(pattern), ls(replacement))))

  // ---- dates & times --------------------------------------------------
  def to_date(e: Column): Column = Column(Ex.Fn("to_date", List(e.ex)))
  def to_date(e: Column, fmt: String): Column = Column(Ex.Fn("to_date", List(e.ex, ls(fmt))))
  def to_timestamp(e: Column): Column = Column(Ex.Fn("to_timestamp", List(e.ex)))
  def to_timestamp(e: Column, fmt: String): Column =
    Column(Ex.Fn("to_timestamp", List(e.ex, ls(fmt))))
  def date_trunc(format: String, e: Column): Column =
    Column(Ex.Fn("date_trunc", List(ls(format), e.ex)))
  def year(e: Column): Column       = Column(Ex.Fn("year", List(e.ex)))
  def month(e: Column): Column      = Column(Ex.Fn("month", List(e.ex)))
  def dayofmonth(e: Column): Column = Column(Ex.Fn("dayofmonth", List(e.ex)))
  def hour(e: Column): Column       = Column(Ex.Fn("hour", List(e.ex)))
  def minute(e: Column): Column     = Column(Ex.Fn("minute", List(e.ex)))
  def second(e: Column): Column     = Column(Ex.Fn("second", List(e.ex)))
  def current_date(): Column        = Column(Ex.Fn("current_date", Nil))
  def current_timestamp(): Column   = Column(Ex.Fn("current_timestamp", Nil))
  def date_add(e: Column, days: Int): Column = Column(Ex.Fn("date_add", List(e.ex, li(days))))
  def date_sub(e: Column, days: Int): Column = Column(Ex.Fn("date_sub", List(e.ex, li(days))))
  def datediff(end: Column, start: Column): Column =
    Column(Ex.Fn("datediff", List(end.ex, start.ex)))

  // ---- math -----------------------------------------------------------
  def abs(e: Column): Column   = Column(Ex.Fn("abs", List(e.ex)))
  def round(e: Column): Column = Column(Ex.Fn("round", List(e.ex)))
  def round(e: Column, scale: Int): Column = Column(Ex.Fn("round", List(e.ex, li(scale))))
  def floor(e: Column): Column = Column(Ex.Fn("floor", List(e.ex)))
  def ceil(e: Column): Column  = Column(Ex.Fn("ceil", List(e.ex)))
  def sqrt(e: Column): Column  = Column(Ex.Fn("sqrt", List(e.ex)))
  def pow(l: Column, r: Column): Column = Column(Ex.Fn("power", List(l.ex, r.ex))) // wire: power
  def exp(e: Column): Column   = Column(Ex.Fn("exp", List(e.ex)))
  def log(e: Column): Column   = Column(Ex.Fn("log", List(e.ex)))
  def greatest(exprs: Column*): Column = Column(Ex.Fn("greatest", exprs.map(_.ex).toList))
  def least(exprs: Column*): Column    = Column(Ex.Fn("least", exprs.map(_.ex).toList))

  // ---- null / conditional ---------------------------------------------
  def coalesce(exprs: Column*): Column = Column(Ex.Fn("coalesce", exprs.map(_.ex).toList))
  def isnull(e: Column): Column        = Column(Ex.Fn("isnull", List(e.ex)))
  def isnan(e: Column): Column         = Column(Ex.Fn("isnan", List(e.ex)))

  /** `when(cond, value)` — chain `.when(...)`/`.otherwise(...)` on the result
    * (see Column). Wire: `Fn("when", [c1, v1, ...])`. */
  def when(condition: Column, value: Column): Column =
    Column(Ex.Fn("when", List(condition.ex, value.ex)))

  // ---- complex types --------------------------------------------------
  def struct(exprs: Column*): Column = Column(Ex.Fn("struct", exprs.map(_.ex).toList))
  def array(exprs: Column*): Column  = Column(Ex.Fn("array", exprs.map(_.ex).toList))
  def map(exprs: Column*): Column    = Column(Ex.Fn("map", exprs.map(_.ex).toList))
  def explode(e: Column): Column       = Column(Ex.Fn("explode", List(e.ex)))
  def explode_outer(e: Column): Column = Column(Ex.Fn("explode_outer", List(e.ex)))
  def size(e: Column): Column          = Column(Ex.Fn("size", List(e.ex)))
  def element_at(e: Column, key: Column): Column = Column(Ex.Fn("element_at", List(e.ex, key.ex)))
  def array_contains(e: Column, value: Column): Column =
    Column(Ex.Fn("array_contains", List(e.ex, value.ex)))
  def map_keys(e: Column): Column   = Column(Ex.Fn("map_keys", List(e.ex)))
  def map_values(e: Column): Column = Column(Ex.Fn("map_values", List(e.ex)))

  // ---- higher-order functions (real Scala lambdas) --------------------
  // The macro extracts the author's source param name into LamVar; at runtime
  // we synthesize the conventional name. Fixtures use the same names to stay
  // render-identical. See FlowExtractor.fnArg lambda branch (lines 436–439).
  def transform(column: Column, f: Column => Column): Column =
    Column(Ex.Fn("transform", List(column.ex, lamEx("x", f))))
  def filter(column: Column, f: Column => Column): Column =
    Column(Ex.Fn("filter", List(column.ex, lamEx("x", f))))
  def forall(column: Column, f: Column => Column): Column =
    Column(Ex.Fn("forall", List(column.ex, lamEx("x", f))))
  def aggregate(column: Column, initialValue: Column, merge: (Column, Column) => Column): Column =
    Column(Ex.Fn("aggregate", List(column.ex, initialValue.ex, lam2Ex("acc", "x", merge))))
  def zip_with(left: Column, right: Column, f: (Column, Column) => Column): Column =
    Column(Ex.Fn("zip_with", List(left.ex, right.ex, lam2Ex("l", "r", f))))

  // ---- misc -----------------------------------------------------------
  def hash(exprs: Column*): Column = Column(Ex.Fn("hash", exprs.map(_.ex).toList))
  def md5(e: Column): Column       = Column(Ex.Fn("md5", List(e.ex)))
  def sha2(e: Column, numBits: Int): Column = Column(Ex.Fn("sha2", List(e.ex, li(numBits))))
  def monotonically_increasing_id(): Column = Column(Ex.Fn("monotonically_increasing_id", Nil))
  def rand(): Column           = Column(Ex.Fn("rand", Nil))
  def rand(seed: Long): Column = Column(Ex.Fn("rand", List(lng(seed))))

  private def lamEx(name: String, f: Column => Column): Ex =
    Ex.Lam(List(name), f(Column(Ex.LamVar(name))).ex)
  private def lam2Ex(p1: String, p2: String, f: (Column, Column) => Column): Ex =
    Ex.Lam(List(p1, p2), f(Column(Ex.LamVar(p1)), Column(Ex.LamVar(p2))).ex)
