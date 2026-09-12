package dev.sdp.dsl

import dev.sdp.core.algebra.*

/** The relation builder: a `Df` wraps one `Rel` tree, and each method appends
  * one algebra node, eagerly, at call time. The surface mirrors Spark's
  * `Dataset`/`DataFrame` (D8), so a flow body reads like Spark code.
  */
final case class Df(rel: Rel):

  // --- projection / filtering ----------------------------------------
  /** `select(cols*)` — Column overload. */
  def select(columns: Column*): Df =
    Df(Rel.Project(rel, columns.map(_.ex).toList))
  /** `select("a", "b")` — Spark's bare-String overload → `Ex.Col`. */
  def select(col1: String, cols: String*): Df =
    Df(Rel.Project(rel, (col1 +: cols).map(Ex.Col(_)).toList))

  /** `where`/`filter`. */
  def where(condition: Column): Df  = Df(Rel.Filter(rel, condition.ex))
  def filter(condition: Column): Df = where(condition)

  // --- grouping ------------------------------------------------------
  def groupBy(columns: Column*): GroupedDf = GroupedDf(rel, columns.map(_.ex).toList)
  def groupBy(col1: String, cols: String*): GroupedDf =
    GroupedDf(rel, (col1 +: cols).map(Ex.Col(_)).toList)

  // --- joins (curried: (right)(on)) ----------------------------------
  def join(right: Df)(on: Column): Df      = Df(Rel.Join(rel, right.rel, Some(on.ex), JoinType.Inner))
  def joinLeft(right: Df)(on: Column): Df  = Df(Rel.Join(rel, right.rel, Some(on.ex), JoinType.LeftOuter))
  def joinRight(right: Df)(on: Column): Df = Df(Rel.Join(rel, right.rel, Some(on.ex), JoinType.RightOuter))
  def joinFull(right: Df)(on: Column): Df  = Df(Rel.Join(rel, right.rel, Some(on.ex), JoinType.FullOuter))
  def joinSemi(right: Df)(on: Column): Df  = Df(Rel.Join(rel, right.rel, Some(on.ex), JoinType.LeftSemi))
  def joinAnti(right: Df)(on: Column): Df  = Df(Rel.Join(rel, right.rel, Some(on.ex), JoinType.LeftAnti))
  /** `crossJoin`: `Join(_, _, None, Cross)`. */
  def crossJoin(right: Df): Df = Df(Rel.Join(rel, right.rel, None, JoinType.Cross))

  // --- set operations
  def union(other: Df): Df        = Df(Rel.SetOp(rel, other.rel, SetOpType.Union, all = true))
  def intersect(other: Df): Df    = Df(Rel.SetOp(rel, other.rel, SetOpType.Intersect, all = false))
  def except(other: Df): Df       = Df(Rel.SetOp(rel, other.rel, SetOpType.Except, all = false))
  def intersectAll(other: Df): Df = Df(Rel.SetOp(rel, other.rel, SetOpType.Intersect, all = true))
  def exceptAll(other: Df): Df    = Df(Rel.SetOp(rel, other.rel, SetOpType.Except, all = true))

  // --- dedup / drop / alias / toDF -----------------------------------
  /** `distinct`: `Deduplicate(_, Nil)`. */
  def distinct: Df = Df(Rel.Deduplicate(rel, Nil))
  /** `dropDuplicates(cols*)`. */
  def dropDuplicates(columns: String*): Df = Df(Rel.Deduplicate(rel, columns.toList))
  /** `drop(cols*)`. */
  def drop(columns: String*): Df = Df(Rel.Drop(rel, columns.toList))
  /** `alias(name)`: `SubqueryAlias`. */
  def alias(name: String): Df = Df(Rel.SubqueryAlias(rel, name))
  /** `toDF(names*)`. */
  def toDF(columnNames: String*): Df = Df(Rel.ToDF(rel, columnNames.toList))

  // --- withColumn(s) / renames ---------------------------------------
  /** Consecutive `withColumn`s collapse into one `WithColumns`. */
  def withColumn(colName: String, c: Column): Df = rel match
    case Rel.WithColumns(inner, cols) => Df(Rel.WithColumns(inner, cols :+ (colName -> c.ex)))
    case other                        => Df(Rel.WithColumns(other, List(colName -> c.ex)))

  /** `withColumns(Map(...))` — Spark's Map signature; source order preserved. */
  def withColumns(colsMap: Map[String, Column]): Df =
    Df(Rel.WithColumns(rel, colsMap.toList.map((k, v) => k -> v.ex)))

  /** Consecutive renames collapse. */
  def withColumnRenamed(existing: String, renamed: String): Df = rel match
    case Rel.WithColumnsRenamed(inner, renames) =>
      Df(Rel.WithColumnsRenamed(inner, renames :+ (existing -> renamed)))
    case other => Df(Rel.WithColumnsRenamed(other, List(existing -> renamed)))

  /** `withColumnsRenamed(Map(...))`. */
  def withColumnsRenamed(colsMap: Map[String, String]): Df =
    Df(Rel.WithColumnsRenamed(rel, colsMap.toList))

  /** `withColumnsRenamed(Seq(...), Seq(...))` — parallel name lists. */
  def withColumnsRenamed(colNames: Seq[String], newColNames: Seq[String]): Df =
    if colNames.sizeIs == newColNames.size then
      Df(Rel.WithColumnsRenamed(rel, colNames.zip(newColNames).toList))
    else throw new IllegalArgumentException("withColumnsRenamed name lists must be the same length")

  // --- sample / hint / partitioning ----------------------------------
  /** `sample(fraction)` / `sample(fraction, seed)`. */
  def sample(fraction: Double): Df =
    Df(Rel.Sample(rel, RelCodec.requireFinite("sample fraction", fraction), None))
  def sample(fraction: Double, seed: Long): Df =
    Df(Rel.Sample(rel, RelCodec.requireFinite("sample fraction", fraction), Some(seed)))
  /** `hint(name, params*)`. */
  def hint(name: String, parameters: Column*): Df =
    Df(Rel.Hint(rel, name, parameters.map(_.ex).toList))
  /** `repartition(n)` (shuffle=true). */
  def repartition(n: Int): Df = Df(Rel.Repartition(rel, n, shuffle = true))
  /** `coalesce(n)` (shuffle=false). */
  def coalesce(n: Int): Df = Df(Rel.Repartition(rel, n, shuffle = false))
  /** `repartitionBy(exprs*)`. */
  def repartitionBy(exprs: Column*): Df =
    Df(Rel.RepartitionByExpression(rel, exprs.map(_.ex).toList, None))

  // --- na/fill (flat spellings) --------------------------------------
  /** `dropNa(cols*)`. */
  def dropNa(columns: String*): Df = Df(Rel.DropNa(rel, columns.toList))
  /** `fillNa(value, cols*)` — value must be a `lit(...)`. */
  def fillNa(value: Column, columns: String*): Df =
    Df(Rel.FillNa(rel, columns.toList, asLit(value, "fillNa value")))

  // --- stat relations (flat spellings) -------------------------------
  /** `describe(cols*)`. */
  def describe(columns: String*): Df = Df(Rel.Describe(rel, columns.toList))
  /** `summary(stats*)`. */
  def summary(statistics: String*): Df = Df(Rel.Summary(rel, statistics.toList))
  /** crosstab/cov/corr. */
  def crosstab(col1: String, col2: String): Df = Df(Rel.Crosstab(rel, col1, col2))
  def cov(col1: String, col2: String): Df       = Df(Rel.Cov(rel, col1, col2))
  def corr(col1: String, col2: String): Df      = Df(Rel.Corr(rel, col1, col2))
  /** `freqItems(cols*)`. */
  def freqItems(columns: String*): Df = Df(Rel.FreqItems(rel, columns.toList))

  // --- unpivot / transpose / replaceValues / observe -----------------
  /** `unpivot(ids*)(var, val)`: empty `values`. */
  def unpivot(ids: Column*)(variableColumnName: String, valueColumnName: String): Df =
    Df(Rel.Unpivot(rel, ids.map(_.ex).toList, Nil, variableColumnName, valueColumnName))
  /** `transpose(idx*)`. */
  def transpose(indexColumns: Column*): Df =
    Df(Rel.Transpose(rel, indexColumns.map(_.ex).toList))
  /** `replaceValues(old, new, cols*)` — both literals. */
  def replaceValues(oldValue: Column, newValue: Column, columns: String*): Df =
    Df(Rel.Replace(
      rel,
      columns.toList,
      List((asLit(oldValue, "replaceValues old value"), asLit(newValue, "replaceValues new value"))),
    ))
  /** `observe(name, metrics*)`. */
  def observe(name: String, metrics: Column*): Df =
    Df(Rel.CollectMetrics(rel, name, metrics.map(_.ex).toList))

  // --- ordering / limits ---------------------------------------------
  /** `orderBy`/`sort` with explicit sort keys (col.asc/col.desc). */
  def orderBy(keys: SortKey*): Df = Df(Rel.Sort(rel, keys.toList))
  /** `orderBy`/`sort` with bare columns — ascending (Spark's default). */
  def orderBy(col1: Column, cols: Column*): Df =
    Df(Rel.Sort(rel, (col1 +: cols).map(c => SortKey(c.ex)).toList))
  /** `orderBy("col", ...)` — string columns, ascending. */
  def orderBy(col1: String, cols: String*): Df =
    Df(Rel.Sort(rel, (col1 +: cols).map(c => SortKey(Ex.Col(c))).toList))
  def sort(keys: SortKey*): Df                   = orderBy(keys*)
  def sort(col1: Column, cols: Column*): Df      = orderBy(col1, cols*)
  def sort(col1: String, cols: String*): Df      = orderBy(col1, cols*)

  /** `limit`/`offset`/`tail`. */
  def limit(n: Int): Df  = Df(Rel.Limit(rel, n))
  def offset(n: Int): Df = Df(Rel.Offset(rel, n))
  def tail(n: Int): Df   = Df(Rel.Tail(rel, n))

  // --- namespaces ----------------------------------------------------
  /** `df.na`. */
  def na: NaFunctionsB = NaFunctionsB(rel)
  /** `df.stat`. */
  def stat: StatFunctionsB = StatFunctionsB(rel)

  private def asLit(c: Column, what: String): LitValue = c.ex match
    case Ex.Lit(v) => v
    case _         => throw new IllegalArgumentException(s"$what must be a lit(...)")

/** `groupBy(...)` awaiting its aggregates. */
final case class GroupedDf(input: Rel, groups: List[Ex]):
  def agg(aggregates: Column*): Df =
    Df(Rel.Aggregate(input, groups, aggregates.map(_.ex).toList))
  /** `groupBy(...).count()` — Spark convenience: one `count` aggregate named
    * `count`. */
  def count(): Df =
    Df(Rel.Aggregate(input, groups, List(Ex.Alias(Ex.Fn("count", List(Ex.Star(None))), "count"))))

/** `df.na.*` — `DataFrameNaFunctions` mirror. */
final case class NaFunctionsB(input: Rel):
  /** `na.drop()` — all columns. */
  def drop(): Df = Df(Rel.DropNa(input, Nil))
  /** `na.drop(Seq(...))` — named columns. */
  def drop(cols: Seq[String]): Df = Df(Rel.DropNa(input, cols.toList))
  /** `na.fill(value)` — all columns. */
  def fill(value: Column): Df = Df(Rel.FillNa(input, Nil, asLit(value, "na.fill value")))
  /** `na.fill(value, Seq(...))`. */
  def fill(value: Column, cols: Seq[String]): Df =
    Df(Rel.FillNa(input, cols.toList, asLit(value, "na.fill value")))
  /** `na.replace("col", Map(lit -> lit))`. */
  def replace(col: String, replacement: Map[Column, Column]): Df =
    Df(Rel.Replace(input, List(col), replacementPairs(replacement)))
  /** `na.replace(Seq("a","b"), Map(lit -> lit))`. */
  def replace(cols: Seq[String], replacement: Map[Column, Column]): Df =
    Df(Rel.Replace(input, cols.toList, replacementPairs(replacement)))

  private def replacementPairs(m: Map[Column, Column]): List[(LitValue, LitValue)] =
    m.toList.map((k, v) => (asLit(k, "na.replace key"), asLit(v, "na.replace value")))
  private def asLit(c: Column, what: String): LitValue = c.ex match
    case Ex.Lit(v) => v
    case _         => throw new IllegalArgumentException(s"$what must be a lit(...)")

/** `df.stat.*` — `DataFrameStatFunctions` mirror. */
final case class StatFunctionsB(input: Rel):
  def crosstab(col1: String, col2: String): Df = Df(Rel.Crosstab(input, col1, col2))
  def cov(col1: String, col2: String): Df       = Df(Rel.Cov(input, col1, col2))
  def corr(col1: String, col2: String): Df      = Df(Rel.Corr(input, col1, col2))
  def freqItems(cols: Seq[String]): Df          = Df(Rel.FreqItems(input, cols.toList))
