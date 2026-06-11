package dev.sdp.dsl

import dev.sdp.core.algebra.*

/** Runtime relation builder: the value-level analogue of the macro's
  * `FlowExtractor.rel` recursive descent (FlowExtractor.scala lines 599–1021).
  * Each method appends one algebra node — same `Rel` shape, eagerly, at call
  * time. The surface mirrors `dev.sdp.dsl.FlowRel` (FlowApi.scala lines 19–87).
  */
final case class Df(rel: Rel):

  // --- projection / filtering ----------------------------------------
  /** `select(cols*)` — Column overload (FlowExtractor lines 758–762). */
  def select(columns: Column*): Df =
    Df(Rel.Project(rel, columns.map(_.ex).toList))
  /** `select("a", "b")` — Spark's bare-String overload → `Ex.Col` (colArg,
    * FlowExtractor lines 288–291). */
  def select(col1: String, cols: String*): Df =
    Df(Rel.Project(rel, (col1 +: cols).map(Ex.Col(_)).toList))

  /** `where`/`filter` (FlowExtractor lines 764–768). */
  def where(condition: Column): Df  = Df(Rel.Filter(rel, condition.ex))
  def filter(condition: Column): Df = where(condition)

  // --- grouping ------------------------------------------------------
  def groupBy(columns: Column*): GroupedDf = GroupedDf(rel, columns.map(_.ex).toList)
  def groupBy(col1: String, cols: String*): GroupedDf =
    GroupedDf(rel, (col1 +: cols).map(Ex.Col(_)).toList)

  // --- joins (curried: (right)(on)) ----------------------------------
  // FlowExtractor lines 785–790 (`Apply(Apply(Select(_, JoinMethod), [right]), [cond])`).
  def join(right: Df)(on: Column): Df      = Df(Rel.Join(rel, right.rel, Some(on.ex), JoinType.Inner))
  def joinLeft(right: Df)(on: Column): Df  = Df(Rel.Join(rel, right.rel, Some(on.ex), JoinType.LeftOuter))
  def joinRight(right: Df)(on: Column): Df = Df(Rel.Join(rel, right.rel, Some(on.ex), JoinType.RightOuter))
  def joinFull(right: Df)(on: Column): Df  = Df(Rel.Join(rel, right.rel, Some(on.ex), JoinType.FullOuter))
  def joinSemi(right: Df)(on: Column): Df  = Df(Rel.Join(rel, right.rel, Some(on.ex), JoinType.LeftSemi))
  def joinAnti(right: Df)(on: Column): Df  = Df(Rel.Join(rel, right.rel, Some(on.ex), JoinType.LeftAnti))
  /** `crossJoin` (FlowExtractor lines 792–796): `Join(_, _, None, Cross)`. */
  def crossJoin(right: Df): Df = Df(Rel.Join(rel, right.rel, None, JoinType.Cross))

  // --- set operations (FlowExtractor.SetOpMethod lines 88–95, applied 798–802)
  def union(other: Df): Df        = Df(Rel.SetOp(rel, other.rel, SetOpType.Union, all = true))
  def intersect(other: Df): Df    = Df(Rel.SetOp(rel, other.rel, SetOpType.Intersect, all = false))
  def except(other: Df): Df       = Df(Rel.SetOp(rel, other.rel, SetOpType.Except, all = false))
  def intersectAll(other: Df): Df = Df(Rel.SetOp(rel, other.rel, SetOpType.Intersect, all = true))
  def exceptAll(other: Df): Df    = Df(Rel.SetOp(rel, other.rel, SetOpType.Except, all = true))

  // --- dedup / drop / alias / toDF -----------------------------------
  /** `distinct` (FlowExtractor lines 804–805): `Deduplicate(_, Nil)`. */
  def distinct: Df = Df(Rel.Deduplicate(rel, Nil))
  /** `dropDuplicates(cols*)` (lines 807–811). */
  def dropDuplicates(columns: String*): Df = Df(Rel.Deduplicate(rel, columns.toList))
  /** `drop(cols*)` (lines 813–817). */
  def drop(columns: String*): Df = Df(Rel.Drop(rel, columns.toList))
  /** `alias(name)` (lines 819–823): `SubqueryAlias`. */
  def alias(name: String): Df = Df(Rel.SubqueryAlias(rel, name))
  /** `toDF(names*)` (lines 825–829). */
  def toDF(columnNames: String*): Df = Df(Rel.ToDF(rel, columnNames.toList))

  // --- withColumn(s) / renames ---------------------------------------
  /** Consecutive `withColumn`s collapse into one `WithColumns` (FlowExtractor
    * lines 831–839). */
  def withColumn(colName: String, c: Column): Df = rel match
    case Rel.WithColumns(inner, cols) => Df(Rel.WithColumns(inner, cols :+ (colName -> c.ex)))
    case other                        => Df(Rel.WithColumns(other, List(colName -> c.ex)))

  /** `withColumns(Map(...))` — Spark's Map signature; source order preserved
    * (FlowExtractor lines 843–847). */
  def withColumns(colsMap: Map[String, Column]): Df =
    Df(Rel.WithColumns(rel, colsMap.toList.map((k, v) => k -> v.ex)))

  /** Consecutive renames collapse (FlowExtractor lines 965–973). */
  def withColumnRenamed(existing: String, renamed: String): Df = rel match
    case Rel.WithColumnsRenamed(inner, renames) =>
      Df(Rel.WithColumnsRenamed(inner, renames :+ (existing -> renamed)))
    case other => Df(Rel.WithColumnsRenamed(other, List(existing -> renamed)))

  /** `withColumnsRenamed(Map(...))` (FlowExtractor lines 976–980). */
  def withColumnsRenamed(colsMap: Map[String, String]): Df =
    Df(Rel.WithColumnsRenamed(rel, colsMap.toList))

  /** `withColumnsRenamed(Seq(...), Seq(...))` — parallel name lists
    * (FlowExtractor lines 983–993). */
  def withColumnsRenamed(colNames: Seq[String], newColNames: Seq[String]): Df =
    if colNames.sizeIs == newColNames.size then
      Df(Rel.WithColumnsRenamed(rel, colNames.zip(newColNames).toList))
    else throw new IllegalArgumentException("withColumnsRenamed name lists must be the same length")

  // --- sample / hint / partitioning ----------------------------------
  /** `sample(fraction)` / `sample(fraction, seed)` (FlowExtractor lines 849–859). */
  def sample(fraction: Double): Df             = Df(Rel.Sample(rel, fraction, None))
  def sample(fraction: Double, seed: Long): Df = Df(Rel.Sample(rel, fraction, Some(seed)))
  /** `hint(name, params*)` (FlowExtractor lines 861–866). */
  def hint(name: String, parameters: Column*): Df =
    Df(Rel.Hint(rel, name, parameters.map(_.ex).toList))
  /** `repartition(n)` (shuffle=true) (FlowExtractor lines 868–876). */
  def repartition(n: Int): Df = Df(Rel.Repartition(rel, n, shuffle = true))
  /** `coalesce(n)` (shuffle=false). */
  def coalesce(n: Int): Df = Df(Rel.Repartition(rel, n, shuffle = false))
  /** `repartitionBy(exprs*)` (FlowExtractor lines 878–882). */
  def repartitionBy(exprs: Column*): Df =
    Df(Rel.RepartitionByExpression(rel, exprs.map(_.ex).toList, None))

  // --- na/fill (flat spellings) --------------------------------------
  /** `dropNa(cols*)` (FlowExtractor lines 884–888). */
  def dropNa(columns: String*): Df = Df(Rel.DropNa(rel, columns.toList))
  /** `fillNa(value, cols*)` — value must be a `lit(...)` (FlowExtractor lines
    * 890–900). */
  def fillNa(value: Column, columns: String*): Df =
    Df(Rel.FillNa(rel, columns.toList, asLit(value, "fillNa value")))

  // --- stat relations (flat spellings) -------------------------------
  /** `describe(cols*)` (FlowExtractor lines 902–906). */
  def describe(columns: String*): Df = Df(Rel.Describe(rel, columns.toList))
  /** `summary(stats*)` (FlowExtractor lines 908–912). */
  def summary(statistics: String*): Df = Df(Rel.Summary(rel, statistics.toList))
  /** crosstab/cov/corr (FlowExtractor lines 914–922). */
  def crosstab(col1: String, col2: String): Df = Df(Rel.Crosstab(rel, col1, col2))
  def cov(col1: String, col2: String): Df       = Df(Rel.Cov(rel, col1, col2))
  def corr(col1: String, col2: String): Df      = Df(Rel.Corr(rel, col1, col2))
  /** `freqItems(cols*)` (FlowExtractor lines 924–928). */
  def freqItems(columns: String*): Df = Df(Rel.FreqItems(rel, columns.toList))

  // --- unpivot / transpose / replaceValues / observe -----------------
  /** `unpivot(ids*)(var, val)` (FlowExtractor lines 930–936): empty `values`. */
  def unpivot(ids: Column*)(variableColumnName: String, valueColumnName: String): Df =
    Df(Rel.Unpivot(rel, ids.map(_.ex).toList, Nil, variableColumnName, valueColumnName))
  /** `transpose(idx*)` (FlowExtractor lines 938–942). */
  def transpose(indexColumns: Column*): Df =
    Df(Rel.Transpose(rel, indexColumns.map(_.ex).toList))
  /** `replaceValues(old, new, cols*)` — both literals (FlowExtractor lines
    * 944–956). */
  def replaceValues(oldValue: Column, newValue: Column, columns: String*): Df =
    Df(Rel.Replace(
      rel,
      columns.toList,
      List((asLit(oldValue, "replaceValues old value"), asLit(newValue, "replaceValues new value"))),
    ))
  /** `observe(name, metrics*)` (FlowExtractor lines 958–963). */
  def observe(name: String, metrics: Column*): Df =
    Df(Rel.CollectMetrics(rel, name, metrics.map(_.ex).toList))

  // --- ordering / limits ---------------------------------------------
  /** `orderBy`/`sort` with explicit sort keys (col.asc/col.desc). */
  def orderBy(keys: SortKey*): Df = Df(Rel.Sort(rel, keys.toList))
  /** `orderBy`/`sort` with bare columns — ascending (Spark default;
    * FlowExtractor.sortKey lines 293–297). */
  def orderBy(col1: Column, cols: Column*): Df =
    Df(Rel.Sort(rel, (col1 +: cols).map(c => SortKey(c.ex)).toList))
  /** `orderBy("col", ...)` — string columns, ascending. */
  def orderBy(col1: String, cols: String*): Df =
    Df(Rel.Sort(rel, (col1 +: cols).map(c => SortKey(Ex.Col(c))).toList))
  def sort(keys: SortKey*): Df                   = orderBy(keys*)
  def sort(col1: Column, cols: Column*): Df      = orderBy(col1, cols*)
  def sort(col1: String, cols: String*): Df      = orderBy(col1, cols*)

  /** `limit`/`offset`/`tail` (FlowExtractor lines 1001–1012). */
  def limit(n: Int): Df  = Df(Rel.Limit(rel, n))
  def offset(n: Int): Df = Df(Rel.Offset(rel, n))
  def tail(n: Int): Df   = Df(Rel.Tail(rel, n))

  // --- namespaces ----------------------------------------------------
  /** `df.na` (FlowExtractor lines 752–753). */
  def na: NaFunctionsB = NaFunctionsB(rel)
  /** `df.stat` (FlowExtractor lines 754–755). */
  def stat: StatFunctionsB = StatFunctionsB(rel)

  private def asLit(c: Column, what: String): LitValue = c.ex match
    case Ex.Lit(v) => v
    case _         => throw new IllegalArgumentException(s"$what must be a lit(...)")

/** `groupBy(...)` awaiting its aggregates (FlowExtractor lines 771–783). */
final case class GroupedDf(input: Rel, groups: List[Ex]):
  def agg(aggregates: Column*): Df =
    Df(Rel.Aggregate(input, groups, aggregates.map(_.ex).toList))
  /** `groupBy(...).count()` — Spark convenience: one `count` aggregate named
    * `count` (FlowExtractor lines 779–783). */
  def count(): Df =
    Df(Rel.Aggregate(input, groups, List(Ex.Alias(Ex.Fn("count", List(Ex.Star(None))), "count"))))

/** `df.na.*` — `DataFrameNaFunctions` mirror (FlowExtractor.naFunction lines
  * 390–411). */
final case class NaFunctionsB(input: Rel):
  /** `na.drop()` — all columns (FlowExtractor line 393). */
  def drop(): Df = Df(Rel.DropNa(input, Nil))
  /** `na.drop(Seq(...))` — named columns (line 394). */
  def drop(cols: Seq[String]): Df = Df(Rel.DropNa(input, cols.toList))
  /** `na.fill(value)` — all columns (line 395). */
  def fill(value: Column): Df = Df(Rel.FillNa(input, Nil, asLit(value, "na.fill value")))
  /** `na.fill(value, Seq(...))` (lines 396–400). */
  def fill(value: Column, cols: Seq[String]): Df =
    Df(Rel.FillNa(input, cols.toList, asLit(value, "na.fill value")))
  /** `na.replace("col", Map(lit -> lit))` (lines 401–407). */
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

/** `df.stat.*` — `DataFrameStatFunctions` mirror (FlowExtractor.statFunction
  * lines 414–429). */
final case class StatFunctionsB(input: Rel):
  def crosstab(col1: String, col2: String): Df = Df(Rel.Crosstab(input, col1, col2))
  def cov(col1: String, col2: String): Df       = Df(Rel.Cov(input, col1, col2))
  def corr(col1: String, col2: String): Df      = Df(Rel.Corr(input, col1, col2))
  def freqItems(cols: Seq[String]): Df          = Df(Rel.FreqItems(input, cols.toList))
