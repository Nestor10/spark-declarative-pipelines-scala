package dev.sdp.core.algebra

import dev.sdp.core.LineCodec

/** Canonical serialization of [[Rel]]/[[Ex]] trees: s-expressions over
  * percent-encoded atoms.
  *
  * Same design contract as the manifest line format: zero dependencies,
  * byte-stable (equal trees render identically — `render` is a pure function
  * of structure), and total parsing with the offending input in the error.
  * This is the form algebra flow bodies take inside the fragment string the
  * plugin's classload-eval exports (`PipelineExport`) and manifest v2 `flow|`
  * lines.
  *
  * Grammar (one node kind per tag, optional fields last):
  * {{{
  * rel  := (read <name> stream|batch) | (sql <query>)
  *       | (project <rel> <ex>*) | (filter <rel> <ex>)
  *       | (join <jointype> <rel> <rel> <ex>?)
  *       | (agg <rel> (groups <ex>*) (aggs <ex>*))
  *       | (sort <rel> <key>*) | (limit <rel> <int>)
  * key  := (key <ex> asc|desc nf|nl|nd)
  * ex   := (col <name>) | (lit bool|i32|i64|f64|str <value>) | (lit null)
  *       | (fn <name> true|false <ex>*) | (alias <ex> <name>)
  * }}}
  */
object RelCodec:

  // ------------------------------------------------------------------
  // rendering
  // ------------------------------------------------------------------

  def render(rel: Rel): String = rel match
    case Rel.NamedTable(name, streaming) =>
      s"(read ${enc(name)} ${if streaming then "stream" else "batch"})"
    case Rel.DataSource(format, options, streaming, schema, schemaDdl) =>
      val opts = options.toList.sortBy(_._1).map((k, v) => s" (opt ${enc(k)} ${enc(v)})").mkString
      val sch =
        if schema.isEmpty then ""
        else schema.map((n, t) => s" (c ${enc(n)} ${colTypeTag(t)})").mkString(" (sch", "", ")")
      val ddl = schemaDdl.fold("")(d => s" (ddl ${enc(d)})")
      s"(src ${enc(format)} ${if streaming then "stream" else "batch"}$opts$sch$ddl)"
    case Rel.Sql(query) =>
      s"(sql ${enc(query)})"
    case Rel.Project(input, columns) =>
      s"(project ${render(input)}${columns.map(c => " " + renderEx(c)).mkString})"
    case Rel.Filter(input, condition) =>
      s"(filter ${render(input)} ${renderEx(condition)})"
    case Rel.Join(left, right, condition, joinType) =>
      val cond = condition.map(c => " " + renderEx(c)).getOrElse("")
      s"(join ${joinTag(joinType)} ${render(left)} ${render(right)}$cond)"
    case Rel.Aggregate(input, groupBy, aggregates) =>
      val gs = groupBy.map(g => " " + renderEx(g)).mkString
      val as = aggregates.map(a => " " + renderEx(a)).mkString
      s"(agg ${render(input)} (groups$gs) (aggs$as))"
    case Rel.Sort(input, order) =>
      s"(sort ${render(input)}${order.map(k => " " + renderKey(k)).mkString})"
    case Rel.Limit(input, n) =>
      s"(limit ${render(input)} $n)"
    case Rel.Range(start, end, step) =>
      s"(range $start $end $step)"
    case Rel.Offset(input, n) =>
      s"(offset ${render(input)} $n)"
    case Rel.Tail(input, n) =>
      s"(tail ${render(input)} $n)"
    case Rel.Deduplicate(input, columns) =>
      s"(dedup ${render(input)}${columns.map(c => " " + enc(c)).mkString})"
    case Rel.Drop(input, columnNames) =>
      s"(dropcols ${render(input)}${columnNames.map(c => " " + enc(c)).mkString})"
    case Rel.SetOp(left, right, op, all) =>
      val tag  = op match
        case SetOpType.Union     => "union"
        case SetOpType.Intersect => "intersect"
        case SetOpType.Except    => "except"
      val mode = if all then "all" else "distinct"
      s"(setop $tag $mode ${render(left)} ${render(right)})"
    case Rel.SubqueryAlias(input, alias) =>
      s"(qalias ${render(input)} ${enc(alias)})"
    case Rel.ToDF(input, columnNames) =>
      s"(todf ${render(input)}${columnNames.map(c => " " + enc(c)).mkString})"
    case Rel.WithColumns(input, columns) =>
      val cols = columns.map((n, e) => s" (wc ${enc(n)} ${renderEx(e)})").mkString
      s"(withcols ${render(input)}$cols)"
    case Rel.WithColumnsRenamed(input, renames) =>
      val rns = renames.map((from, to) => s" (rn ${enc(from)} ${enc(to)})").mkString
      s"(renames ${render(input)}$rns)"
    case Rel.Sample(input, fraction, seed) =>
      s"(sample ${render(input)} $fraction ${seed.map(_.toString).getOrElse("noseed")})"
    case Rel.Hint(input, name, parameters) =>
      s"(hint ${render(input)} ${enc(name)}${parameters.map(p => " " + renderEx(p)).mkString})"
    case Rel.Repartition(input, n, shuffle) =>
      s"(repartition ${render(input)} $n ${if shuffle then "shuffle" else "coalesce"})"
    case Rel.RepartitionByExpression(input, exprs, n) =>
      s"(repartitionby ${render(input)} ${n.map(_.toString).getOrElse("auto")}${exprs.map(e => " " + renderEx(e)).mkString})"
    case Rel.DropNa(input, cols) =>
      s"(dropna ${render(input)}${cols.map(c => " " + enc(c)).mkString})"
    case Rel.FillNa(input, cols, value) =>
      s"(fillna ${render(input)} ${renderEx(Ex.Lit(value))}${cols.map(c => " " + enc(c)).mkString})"
    case Rel.Describe(input, cols) =>
      s"(describe ${render(input)}${cols.map(c => " " + enc(c)).mkString})"
    case Rel.Summary(input, statistics) =>
      s"(summary ${render(input)}${statistics.map(s => " " + enc(s)).mkString})"
    case Rel.Crosstab(input, c1, c2) =>
      s"(crosstab ${render(input)} ${enc(c1)} ${enc(c2)})"
    case Rel.Cov(input, c1, c2) =>
      s"(cov ${render(input)} ${enc(c1)} ${enc(c2)})"
    case Rel.Corr(input, c1, c2) =>
      s"(corr ${render(input)} ${enc(c1)} ${enc(c2)})"
    case Rel.FreqItems(input, cols) =>
      s"(freqitems ${render(input)}${cols.map(c => " " + enc(c)).mkString})"
    case Rel.Unpivot(input, ids, values, varCol, valCol) =>
      val is = ids.map(e => " " + renderEx(e)).mkString
      val vs = values.map(e => " " + renderEx(e)).mkString
      s"(unpivot ${render(input)} ${enc(varCol)} ${enc(valCol)} (ids$is) (vals$vs))"
    case Rel.Transpose(input, indexColumns) =>
      s"(transpose ${render(input)}${indexColumns.map(e => " " + renderEx(e)).mkString})"
    case Rel.Replace(input, cols, replacements) =>
      val rs = replacements.map((o, n) => s" (re ${renderEx(Ex.Lit(o))} ${renderEx(Ex.Lit(n))})").mkString
      s"(replace ${render(input)} (cols${cols.map(c => " " + enc(c)).mkString})$rs)"
    case Rel.SampleBy(input, col, fractions, seed) =>
      val fs = fractions.map((s, f) => s" (frac ${renderEx(Ex.Lit(s))} $f)").mkString
      s"(sampleby ${render(input)} ${renderEx(col)} ${seed.map(_.toString).getOrElse("noseed")}$fs)"
    case Rel.ApproxQuantile(input, cols, probabilities, relativeError) =>
      val cs = cols.map(c => " " + enc(c)).mkString
      val ps = probabilities.map(p => " " + p.toString).mkString
      s"(quantile ${render(input)} $relativeError (cols$cs) (probs$ps))"
    case Rel.CollectMetrics(input, name, metrics) =>
      s"(metrics ${render(input)} ${enc(name)}${metrics.map(e => " " + renderEx(e)).mkString})"
    case Rel.AsOfJoin(left, right, lk, rk, jt, dir, exact, tolerance) =>
      val tol = tolerance.map(t => " " + renderEx(t)).getOrElse("")
      s"(asof ${enc(jt)} ${enc(dir)} ${if exact then "exact" else "inexact"} ${render(left)} ${render(right)} ${renderEx(lk)} ${renderEx(rk)}$tol)"
    case Rel.LateralJoin(left, right, condition, jt) =>
      val cond = condition.map(c => " " + renderEx(c)).getOrElse("")
      s"(lateral ${joinTag(jt)} ${render(left)} ${render(right)}$cond)"
    case Rel.Parse(input, format, options) =>
      val fmt = format match
        case ParseFormat.Csv => "csv"; case ParseFormat.Json => "json"
      val opts = options.toList.sortBy(_._1).map((k, v) => s" (opt ${enc(k)} ${enc(v)})").mkString
      s"(parse $fmt ${render(input)}$opts)"
    case Rel.ToSchema(input, schema) =>
      s"(toschema ${render(input)}${schema.map((n, t) => s" (c ${enc(n)} ${colTypeTag(t)})").mkString})"
    case Rel.LocalRelation(schema) =>
      s"(localrel${schema.map((n, t) => s" (c ${enc(n)} ${colTypeTag(t)})").mkString})"
    case Rel.LocalData(schema, rows) =>
      val sch = schema.map((n, t) => s" (c ${enc(n)} ${colTypeTag(t)})").mkString(" (sch", "", ")")
      val rs  = rows.map(r => r.map(v => " " + renderEx(Ex.Lit(v))).mkString(" (row", "", ")")).mkString
      s"(localdata$sch$rs)"
    case Rel.ShowString(input, numRows, truncate, vertical) =>
      s"(shows ${render(input)} $numRows $truncate ${if vertical then "v" else "h"})"
    case Rel.HtmlString(input, numRows, truncate) =>
      s"(htmls ${render(input)} $numRows $truncate)"
    case Rel.Tvf(name, args) =>
      s"(tvf ${enc(name)}${args.map(a => " " + renderEx(a)).mkString})"
    case Rel.Catalog(op) =>
      val tag = op match
        case CatalogOp.CurrentDatabase => "currentdb"
        case CatalogOp.ListDatabases   => "listdbs"
        case CatalogOp.ListTables      => "listtables"
      s"(catalog $tag)"

  def renderEx(ex: Ex): String = ex match
    case Ex.Col(name) => s"(col ${enc(name)})"
    case Ex.Lit(LitValue.Bool(v)) => s"(lit bool $v)"
    case Ex.Lit(LitValue.I32(v))  => s"(lit i32 $v)"
    case Ex.Lit(LitValue.I64(v))  => s"(lit i64 $v)"
    case Ex.Lit(LitValue.F64(v))  => s"(lit f64 $v)"
    case Ex.Lit(LitValue.Str(v))  => s"(lit str ${enc(v)})"
    case Ex.Lit(LitValue.Null)    => "(lit null)"
    case Ex.Fn(name, args, distinct) =>
      s"(fn ${enc(name)} $distinct${args.map(a => " " + renderEx(a)).mkString})"
    case Ex.Alias(expr, name) =>
      s"(alias ${renderEx(expr)} ${enc(name)})"
    case Ex.Cast(expr, to) =>
      s"(cast ${renderEx(expr)} ${colTypeTag(to)})"
    case Ex.ExprString(sql) =>
      s"(expr ${enc(sql)})"
    case Ex.Star(target) =>
      target.map(t => s"(star ${enc(t)})").getOrElse("(star)")
    case Ex.ExtractValue(child, extraction) =>
      s"(extract ${renderEx(child)} ${renderEx(extraction)})"
    case Ex.ColRegex(pattern) =>
      s"(colregex ${enc(pattern)})"
    case Ex.CallFn(name, args) =>
      s"(callfn ${enc(name)}${args.map(a => " " + renderEx(a)).mkString})"
    case Ex.Window(function, partitionBy, orderBy, frame) =>
      val ps = partitionBy.map(p => " " + renderEx(p)).mkString
      val os = orderBy.map(k => " " + renderKey(k)).mkString
      val fr = frame.map(f => " " + renderFrame(f)).getOrElse("")
      s"(window ${renderEx(function)} (parts$ps) (keys$os)$fr)"
    case Ex.Lam(params, body) =>
      s"(lam (vars${params.map(p => " " + enc(p)).mkString}) ${renderEx(body)})"
    case Ex.LamVar(name) =>
      s"(lamvar ${enc(name)})"
    case Ex.Subquery(rel, kind) =>
      kind match
        case SubqueryKind.Scalar => s"(subq scalar ${render(rel)})"
        case SubqueryKind.Exists => s"(subq exists ${render(rel)})"
        case SubqueryKind.In(values) =>
          s"(subq in ${render(rel)} (vals${values.map(v => " " + renderEx(v)).mkString}))"

  private def renderFrame(f: WindowFrame): String =
    s"(frame ${if f.rowFrame then "rows" else "range"} ${renderBoundary(f.lower)} ${renderBoundary(f.upper)})"

  private def renderBoundary(b: FrameBoundary): String = b match
    case FrameBoundary.CurrentRow  => "(cur)"
    case FrameBoundary.Unbounded   => "(unbounded)"
    case FrameBoundary.Value(expr) => s"(val ${renderEx(expr)})"

  private def renderKey(key: SortKey): String =
    val dir   = if key.descending then "desc" else "asc"
    val nulls = key.nullsFirst match
      case Some(true)  => "nf"
      case Some(false) => "nl"
      case None        => "nd"
    s"(key ${renderEx(key.expr)} $dir $nulls)"

  private def joinTag(jt: JoinType): String = jt match
    case JoinType.Inner      => "inner"
    case JoinType.FullOuter  => "full"
    case JoinType.LeftOuter  => "left"
    case JoinType.RightOuter => "right"
    case JoinType.LeftAnti   => "anti"
    case JoinType.LeftSemi   => "semi"
    case JoinType.Cross      => "cross"

  // ------------------------------------------------------------------
  // parsing — tokenizer + recursive descent over Sexp
  // ------------------------------------------------------------------

  def parse(text: String): Either[String, Rel] =
    for
      sexp <- Sexp.read(text)
      rel  <- rel(sexp)
    yield rel

  /** Parse a single expression s-expr (the inverse of [[renderEx]]). Used by
    * [[dev.sdp.core.FlowCodec]] to (de)serialize AUTO CDC key/sequence/filter
    * expressions, which are `Ex` trees outside any `Rel`. */
  def parseEx(text: String): Either[String, Ex] =
    for
      sexp <- Sexp.read(text)
      e    <- ex(sexp)
    yield e

  private enum Sexp:
    case Atom(value: String)
    case ListOf(items: List[Sexp])

  private object Sexp:
    def read(text: String): Either[String, Sexp] =
      val tokens = tokenize(text)
      readOne(tokens).flatMap {
        case (sexp, Nil)  => Right(sexp)
        case (_, leftover) => Left(s"trailing input: ${leftover.take(5).mkString(" ")}")
      }

    private def tokenize(text: String): List[String] =
      text
        .replace("(", " ( ")
        .replace(")", " ) ")
        .split("\\s+")
        .toList
        .filter(_.nonEmpty)

    private def readOne(tokens: List[String]): Either[String, (Sexp, List[String])] =
      tokens match
        case Nil => Left("unexpected end of input")
        case "(" :: rest =>
          def loop(ts: List[String], acc: List[Sexp]): Either[String, (Sexp, List[String])] =
            ts match
              case ")" :: tail => Right((ListOf(acc.reverse), tail))
              case Nil         => Left("unclosed '('")
              case other       => readOne(other).flatMap((item, tail) => loop(tail, item :: acc))
          loop(rest, Nil)
        case ")" :: _    => Left("unexpected ')'")
        case atom :: rest => Right((Atom(atom), rest))

  private def rel(sexp: Sexp): Either[String, Rel] = sexp match
    case Sexp.ListOf(Sexp.Atom("read") :: Sexp.Atom(name) :: Sexp.Atom(mode) :: Nil) =>
      Right(Rel.NamedTable(dec(name), mode == "stream"))
    case Sexp.ListOf(Sexp.Atom("src") :: Sexp.Atom(format) :: Sexp.Atom(mode) :: rest) =>
      val schemaForms = rest.filter { case Sexp.ListOf(Sexp.Atom("sch") :: _) => true; case _ => false }
      val ddlForms    = rest.collect { case Sexp.ListOf(Sexp.Atom("ddl") :: Sexp.Atom(d) :: Nil) => dec(d) }
      val optForms = rest.filter {
        case Sexp.ListOf(Sexp.Atom("sch") :: _) => false
        case Sexp.ListOf(Sexp.Atom("ddl") :: _) => false
        case _                                  => true
      }
      for
        opts <- traverse(optForms) {
          case Sexp.ListOf(Sexp.Atom("opt") :: Sexp.Atom(k) :: Sexp.Atom(v) :: Nil) =>
            Right(dec(k) -> dec(v))
          case other => Left(s"unrecognized source option form: ${describe(other)}")
        }
        schema <- schemaForms match
          case Nil => Right(Nil)
          case Sexp.ListOf(Sexp.Atom("sch") :: cols) :: Nil =>
            traverse(cols) {
              case Sexp.ListOf(Sexp.Atom("c") :: Sexp.Atom(n) :: Sexp.Atom(t) :: Nil) =>
                parseColType(t).map(dec(n) -> _)
              case other => Left(s"unrecognized schema column form: ${describe(other)}")
            }
          case _ => Left("multiple (sch ...) blocks in one source")
      yield Rel.DataSource(dec(format), opts.toMap, mode == "stream", schema, ddlForms.headOption)
    case Sexp.ListOf(Sexp.Atom("sql") :: Sexp.Atom(query) :: Nil) =>
      Right(Rel.Sql(dec(query)))
    case Sexp.ListOf(Sexp.Atom("project") :: input :: columns) =>
      for { i <- rel(input); cs <- traverse(columns)(ex) } yield Rel.Project(i, cs)
    case Sexp.ListOf(Sexp.Atom("filter") :: input :: cond :: Nil) =>
      for { i <- rel(input); c <- ex(cond) } yield Rel.Filter(i, c)
    case Sexp.ListOf(Sexp.Atom("join") :: Sexp.Atom(jt) :: left :: right :: rest) =>
      for
        j <- joinType(jt)
        l <- rel(left)
        r <- rel(right)
        c <- rest match
          case Nil      => Right(None)
          case e :: Nil => ex(e).map(Some(_))
          case _        => Left(s"join: too many arguments")
      yield Rel.Join(l, r, c, j)
    case Sexp.ListOf(
          Sexp.Atom("agg") :: input ::
          Sexp.ListOf(Sexp.Atom("groups") :: groups) ::
          Sexp.ListOf(Sexp.Atom("aggs") :: aggs) :: Nil
        ) =>
      for { i <- rel(input); gs <- traverse(groups)(ex); as <- traverse(aggs)(ex) }
      yield Rel.Aggregate(i, gs, as)
    case Sexp.ListOf(Sexp.Atom("sort") :: input :: keys) =>
      for { i <- rel(input); ks <- traverse(keys)(sortKey) } yield Rel.Sort(i, ks)
    case Sexp.ListOf(Sexp.Atom("limit") :: input :: Sexp.Atom(n) :: Nil) =>
      for
        i <- rel(input)
        v <- n.toIntOption.toRight(s"limit: not an int: $n")
      yield Rel.Limit(i, v)
    case Sexp.ListOf(Sexp.Atom("range") :: Sexp.Atom(s) :: Sexp.Atom(e) :: Sexp.Atom(st) :: Nil) =>
      for
        start <- s.toLongOption.toRight(s"range start: $s")
        end   <- e.toLongOption.toRight(s"range end: $e")
        step  <- st.toLongOption.toRight(s"range step: $st")
      yield Rel.Range(start, end, step)
    case Sexp.ListOf(Sexp.Atom("offset") :: input :: Sexp.Atom(n) :: Nil) =>
      for { i <- rel(input); v <- n.toIntOption.toRight(s"offset: not an int: $n") }
      yield Rel.Offset(i, v)
    case Sexp.ListOf(Sexp.Atom("tail") :: input :: Sexp.Atom(n) :: Nil) =>
      for { i <- rel(input); v <- n.toIntOption.toRight(s"tail: not an int: $n") }
      yield Rel.Tail(i, v)
    case Sexp.ListOf(Sexp.Atom("dedup") :: input :: cols) =>
      for { i <- rel(input); cs <- traverse(cols)(atom("dedup column")) }
      yield Rel.Deduplicate(i, cs)
    case Sexp.ListOf(Sexp.Atom("dropcols") :: input :: cols) =>
      for { i <- rel(input); cs <- traverse(cols)(atom("drop column")) }
      yield Rel.Drop(i, cs)
    case Sexp.ListOf(Sexp.Atom("setop") :: Sexp.Atom(tag) :: Sexp.Atom(mode) :: left :: right :: Nil) =>
      for
        op <- tag match
          case "union"     => Right(SetOpType.Union)
          case "intersect" => Right(SetOpType.Intersect)
          case "except"    => Right(SetOpType.Except)
          case other       => Left(s"unknown set-op type: $other")
        all <- mode match
          case "all" => Right(true); case "distinct" => Right(false)
          case other => Left(s"unknown set-op mode: $other")
        l <- rel(left)
        r <- rel(right)
      yield Rel.SetOp(l, r, op, all)
    case Sexp.ListOf(Sexp.Atom("qalias") :: input :: Sexp.Atom(alias) :: Nil) =>
      rel(input).map(Rel.SubqueryAlias(_, dec(alias)))
    case Sexp.ListOf(Sexp.Atom("todf") :: input :: cols) =>
      for { i <- rel(input); cs <- traverse(cols)(atom("toDF column")) }
      yield Rel.ToDF(i, cs)
    case Sexp.ListOf(Sexp.Atom("withcols") :: input :: cols) =>
      for
        i <- rel(input)
        cs <- traverse(cols) {
          case Sexp.ListOf(Sexp.Atom("wc") :: Sexp.Atom(n) :: e :: Nil) => ex(e).map(dec(n) -> _)
          case other => Left(s"unrecognized withcols form: ${describe(other)}")
        }
      yield Rel.WithColumns(i, cs)
    case Sexp.ListOf(Sexp.Atom("sample") :: input :: Sexp.Atom(fraction) :: Sexp.Atom(seed) :: Nil) =>
      for
        i <- rel(input)
        f <- fraction.toDoubleOption.toRight(s"sample fraction: $fraction")
        s <- seed match
          case "noseed" => Right(None)
          case v        => v.toLongOption.map(Some(_)).toRight(s"sample seed: $v")
      yield Rel.Sample(i, f, s)
    case Sexp.ListOf(Sexp.Atom("hint") :: input :: Sexp.Atom(name) :: params) =>
      for { i <- rel(input); ps <- traverse(params)(ex) } yield Rel.Hint(i, dec(name), ps)
    case Sexp.ListOf(Sexp.Atom("repartition") :: input :: Sexp.Atom(n) :: Sexp.Atom(mode) :: Nil) =>
      for
        i <- rel(input)
        v <- n.toIntOption.toRight(s"repartition: not an int: $n")
      yield Rel.Repartition(i, v, mode == "shuffle")
    case Sexp.ListOf(Sexp.Atom("repartitionby") :: input :: Sexp.Atom(n) :: exprs) =>
      for
        i  <- rel(input)
        np <- if n == "auto" then Right(None) else n.toIntOption.map(Some(_)).toRight(s"repartitionby: $n")
        es <- traverse(exprs)(ex)
      yield Rel.RepartitionByExpression(i, es, np)
    case Sexp.ListOf(Sexp.Atom("dropna") :: input :: cols) =>
      for { i <- rel(input); cs <- traverse(cols)(atom("dropna column")) } yield Rel.DropNa(i, cs)
    case Sexp.ListOf(Sexp.Atom("fillna") :: input :: lit :: cols) =>
      for
        i <- rel(input)
        v <- ex(lit).flatMap { case Ex.Lit(value) => Right(value); case other => Left(s"fillna: not a literal") }
        cs <- traverse(cols)(atom("fillna column"))
      yield Rel.FillNa(i, cs, v)
    case Sexp.ListOf(Sexp.Atom("describe") :: input :: cols) =>
      for { i <- rel(input); cs <- traverse(cols)(atom("describe column")) } yield Rel.Describe(i, cs)
    case Sexp.ListOf(Sexp.Atom("summary") :: input :: stats) =>
      for { i <- rel(input); ss <- traverse(stats)(atom("summary statistic")) } yield Rel.Summary(i, ss)
    case Sexp.ListOf(Sexp.Atom("crosstab") :: input :: Sexp.Atom(c1) :: Sexp.Atom(c2) :: Nil) =>
      rel(input).map(Rel.Crosstab(_, dec(c1), dec(c2)))
    case Sexp.ListOf(Sexp.Atom("cov") :: input :: Sexp.Atom(c1) :: Sexp.Atom(c2) :: Nil) =>
      rel(input).map(Rel.Cov(_, dec(c1), dec(c2)))
    case Sexp.ListOf(Sexp.Atom("corr") :: input :: Sexp.Atom(c1) :: Sexp.Atom(c2) :: Nil) =>
      rel(input).map(Rel.Corr(_, dec(c1), dec(c2)))
    case Sexp.ListOf(Sexp.Atom("freqitems") :: input :: cols) =>
      for { i <- rel(input); cs <- traverse(cols)(atom("freqitems column")) } yield Rel.FreqItems(i, cs)
    case Sexp.ListOf(
          Sexp.Atom("unpivot") :: input :: Sexp.Atom(varCol) :: Sexp.Atom(valCol) ::
          Sexp.ListOf(Sexp.Atom("ids") :: ids) :: Sexp.ListOf(Sexp.Atom("vals") :: vals) :: Nil
        ) =>
      for
        i  <- rel(input)
        is <- traverse(ids)(ex)
        vs <- traverse(vals)(ex)
      yield Rel.Unpivot(i, is, vs, dec(varCol), dec(valCol))
    case Sexp.ListOf(Sexp.Atom("transpose") :: input :: idx) =>
      for { i <- rel(input); es <- traverse(idx)(ex) } yield Rel.Transpose(i, es)
    case Sexp.ListOf(Sexp.Atom("replace") :: input :: Sexp.ListOf(Sexp.Atom("cols") :: cols) :: rs) =>
      for
        i  <- rel(input)
        cs <- traverse(cols)(atom("replace column"))
        reps <- traverse(rs) {
          case Sexp.ListOf(Sexp.Atom("re") :: o :: n :: Nil) =>
            for
              ov <- ex(o).flatMap { case Ex.Lit(v) => Right(v); case _ => Left("replace: old not literal") }
              nv <- ex(n).flatMap { case Ex.Lit(v) => Right(v); case _ => Left("replace: new not literal") }
            yield (ov, nv)
          case other => Left(s"unrecognized replacement form: ${describe(other)}")
        }
      yield Rel.Replace(i, cs, reps)
    case Sexp.ListOf(Sexp.Atom("sampleby") :: input :: col :: Sexp.Atom(seed) :: fs) =>
      for
        i <- rel(input)
        c <- ex(col)
        s <- seed match
          case "noseed" => Right(None)
          case v        => v.toLongOption.map(Some(_)).toRight(s"sampleby seed: $v")
        fracs <- traverse(fs) {
          case Sexp.ListOf(Sexp.Atom("frac") :: st :: Sexp.Atom(f) :: Nil) =>
            for
              sv <- ex(st).flatMap { case Ex.Lit(v) => Right(v); case _ => Left("sampleby: stratum not literal") }
              fv <- f.toDoubleOption.toRight(s"sampleby fraction: $f")
            yield (sv, fv)
          case other => Left(s"unrecognized fraction form: ${describe(other)}")
        }
      yield Rel.SampleBy(i, c, fracs, s)
    case Sexp.ListOf(
          Sexp.Atom("quantile") :: input :: Sexp.Atom(err) ::
          Sexp.ListOf(Sexp.Atom("cols") :: cols) :: Sexp.ListOf(Sexp.Atom("probs") :: probs) :: Nil
        ) =>
      for
        i  <- rel(input)
        e  <- err.toDoubleOption.toRight(s"quantile relativeError: $err")
        cs <- traverse(cols)(atom("quantile column"))
        ps <- traverse(probs) {
          case Sexp.Atom(p) => p.toDoubleOption.toRight(s"quantile probability: $p")
          case other        => Left(s"quantile probability must be an atom: ${describe(other)}")
        }
      yield Rel.ApproxQuantile(i, cs, ps, e)
    case Sexp.ListOf(Sexp.Atom("metrics") :: input :: Sexp.Atom(name) :: ms) =>
      for { i <- rel(input); es <- traverse(ms)(ex) } yield Rel.CollectMetrics(i, dec(name), es)
    case Sexp.ListOf(
          Sexp.Atom("asof") :: Sexp.Atom(jt) :: Sexp.Atom(dir) :: Sexp.Atom(exact) ::
          left :: right :: lk :: rk :: rest
        ) =>
      for
        l  <- rel(left)
        r  <- rel(right)
        lA <- ex(lk)
        rA <- ex(rk)
        tol <- rest match
          case Nil      => Right(None)
          case t :: Nil => ex(t).map(Some(_))
          case _        => Left("asof: too many arguments")
      yield Rel.AsOfJoin(l, r, lA, rA, dec(jt), dec(dir), exact == "exact", tol)
    case Sexp.ListOf(Sexp.Atom("lateral") :: Sexp.Atom(jt) :: left :: right :: rest) =>
      for
        j <- joinType(jt)
        l <- rel(left)
        r <- rel(right)
        c <- rest match
          case Nil      => Right(None)
          case e :: Nil => ex(e).map(Some(_))
          case _        => Left("lateral: too many arguments")
      yield Rel.LateralJoin(l, r, c, j)
    case Sexp.ListOf(Sexp.Atom("parse") :: Sexp.Atom(fmt) :: input :: opts) =>
      for
        f <- fmt match
          case "csv" => Right(ParseFormat.Csv); case "json" => Right(ParseFormat.Json)
          case other => Left(s"unknown parse format: $other")
        i <- rel(input)
        os <- traverse(opts) {
          case Sexp.ListOf(Sexp.Atom("opt") :: Sexp.Atom(k) :: Sexp.Atom(v) :: Nil) =>
            Right(dec(k) -> dec(v))
          case other => Left(s"unrecognized parse option form: ${describe(other)}")
        }
      yield Rel.Parse(i, f, os.toMap)
    case Sexp.ListOf(Sexp.Atom("toschema") :: input :: cols) =>
      for { i <- rel(input); cs <- traverse(cols)(schemaCol) } yield Rel.ToSchema(i, cs)
    case Sexp.ListOf(Sexp.Atom("localrel") :: cols) =>
      traverse(cols)(schemaCol).map(Rel.LocalRelation(_))
    case Sexp.ListOf(Sexp.Atom("localdata") :: Sexp.ListOf(Sexp.Atom("sch") :: cols) :: rows) =>
      for
        schema <- traverse(cols)(schemaCol)
        rs <- traverse(rows) {
          case Sexp.ListOf(Sexp.Atom("row") :: cells) => traverse(cells)(litCell)
          case other => Left(s"unrecognized localdata row form: ${describe(other)}")
        }
      yield Rel.LocalData(schema, rs)
    case Sexp.ListOf(Sexp.Atom("shows") :: input :: Sexp.Atom(n) :: Sexp.Atom(t) :: Sexp.Atom(v) :: Nil) =>
      for
        i  <- rel(input)
        nr <- n.toIntOption.toRight(s"shows numRows: $n")
        tr <- t.toIntOption.toRight(s"shows truncate: $t")
      yield Rel.ShowString(i, nr, tr, v == "v")
    case Sexp.ListOf(Sexp.Atom("htmls") :: input :: Sexp.Atom(n) :: Sexp.Atom(t) :: Nil) =>
      for
        i  <- rel(input)
        nr <- n.toIntOption.toRight(s"htmls numRows: $n")
        tr <- t.toIntOption.toRight(s"htmls truncate: $t")
      yield Rel.HtmlString(i, nr, tr)
    case Sexp.ListOf(Sexp.Atom("tvf") :: Sexp.Atom(name) :: args) =>
      traverse(args)(ex).map(Rel.Tvf(dec(name), _))
    case Sexp.ListOf(Sexp.Atom("catalog") :: Sexp.Atom(tag) :: Nil) =>
      tag match
        case "currentdb"  => Right(Rel.Catalog(CatalogOp.CurrentDatabase))
        case "listdbs"    => Right(Rel.Catalog(CatalogOp.ListDatabases))
        case "listtables" => Right(Rel.Catalog(CatalogOp.ListTables))
        case other        => Left(s"unknown catalog op: $other")
    case Sexp.ListOf(Sexp.Atom("renames") :: input :: rns) =>
      for
        i <- rel(input)
        rs <- traverse(rns) {
          case Sexp.ListOf(Sexp.Atom("rn") :: Sexp.Atom(from) :: Sexp.Atom(to) :: Nil) =>
            Right(dec(from) -> dec(to))
          case other => Left(s"unrecognized rename form: ${describe(other)}")
        }
      yield Rel.WithColumnsRenamed(i, rs)
    case other => Left(s"unrecognized relation form: ${describe(other)}")

  private def ex(sexp: Sexp): Either[String, Ex] = sexp match
    case Sexp.ListOf(Sexp.Atom("col") :: Sexp.Atom(name) :: Nil) =>
      Right(Ex.Col(dec(name)))
    case Sexp.ListOf(Sexp.Atom("lit") :: Sexp.Atom("null") :: Nil) =>
      Right(Ex.Lit(LitValue.Null))
    case Sexp.ListOf(Sexp.Atom("lit") :: Sexp.Atom(tag) :: Sexp.Atom(v) :: Nil) =>
      tag match
        case "bool" => v.toBooleanOption.map(b => Ex.Lit(LitValue.Bool(b))).toRight(s"lit bool: $v")
        case "i32"  => v.toIntOption.map(i => Ex.Lit(LitValue.I32(i))).toRight(s"lit i32: $v")
        case "i64"  => v.toLongOption.map(l => Ex.Lit(LitValue.I64(l))).toRight(s"lit i64: $v")
        case "f64"  => v.toDoubleOption.map(d => Ex.Lit(LitValue.F64(d))).toRight(s"lit f64: $v")
        case "str"  => Right(Ex.Lit(LitValue.Str(dec(v))))
        case other  => Left(s"unknown literal tag: $other")
    case Sexp.ListOf(Sexp.Atom("fn") :: Sexp.Atom(name) :: Sexp.Atom(distinct) :: args) =>
      for
        d  <- distinct.toBooleanOption.toRight(s"fn distinct flag: $distinct")
        as <- traverse(args)(ex)
      yield Ex.Fn(dec(name), as, d)
    case Sexp.ListOf(Sexp.Atom("alias") :: inner :: Sexp.Atom(name) :: Nil) =>
      ex(inner).map(Ex.Alias(_, dec(name)))
    case Sexp.ListOf(Sexp.Atom("cast") :: inner :: Sexp.Atom(tag) :: Nil) =>
      for { e <- ex(inner); t <- parseColType(tag) } yield Ex.Cast(e, t)
    case Sexp.ListOf(Sexp.Atom("expr") :: Sexp.Atom(sql) :: Nil) =>
      Right(Ex.ExprString(dec(sql)))
    case Sexp.ListOf(Sexp.Atom("star") :: Nil) =>
      Right(Ex.Star(None))
    case Sexp.ListOf(Sexp.Atom("star") :: Sexp.Atom(target) :: Nil) =>
      Right(Ex.Star(Some(dec(target))))
    case Sexp.ListOf(Sexp.Atom("extract") :: child :: extraction :: Nil) =>
      for { c <- ex(child); e <- ex(extraction) } yield Ex.ExtractValue(c, e)
    case Sexp.ListOf(Sexp.Atom("colregex") :: Sexp.Atom(pattern) :: Nil) =>
      Right(Ex.ColRegex(dec(pattern)))
    case Sexp.ListOf(Sexp.Atom("callfn") :: Sexp.Atom(name) :: args) =>
      traverse(args)(ex).map(Ex.CallFn(dec(name), _))
    case Sexp.ListOf(Sexp.Atom("lam") :: Sexp.ListOf(Sexp.Atom("vars") :: vars) :: body :: Nil) =>
      for
        ps <- traverse(vars)(atom("lambda parameter"))
        b  <- ex(body)
      yield Ex.Lam(ps, b)
    case Sexp.ListOf(Sexp.Atom("lamvar") :: Sexp.Atom(name) :: Nil) =>
      Right(Ex.LamVar(dec(name)))
    case Sexp.ListOf(Sexp.Atom("subq") :: Sexp.Atom("scalar") :: relForm :: Nil) =>
      rel(relForm).map(Ex.Subquery(_, SubqueryKind.Scalar))
    case Sexp.ListOf(Sexp.Atom("subq") :: Sexp.Atom("exists") :: relForm :: Nil) =>
      rel(relForm).map(Ex.Subquery(_, SubqueryKind.Exists))
    case Sexp.ListOf(
          Sexp.Atom("subq") :: Sexp.Atom("in") :: relForm ::
          Sexp.ListOf(Sexp.Atom("vals") :: values) :: Nil
        ) =>
      for
        r  <- rel(relForm)
        vs <- traverse(values)(ex)
      yield Ex.Subquery(r, SubqueryKind.In(vs))
    case Sexp.ListOf(
          Sexp.Atom("window") :: fn :: Sexp.ListOf(Sexp.Atom("parts") :: parts) ::
          Sexp.ListOf(Sexp.Atom("keys") :: keys) :: rest
        ) =>
      for
        f  <- ex(fn)
        ps <- traverse(parts)(ex)
        ks <- traverse(keys)(sortKey)
        fr <- rest match
          case Nil           => Right(None)
          case frame :: Nil  => parseFrame(frame).map(Some(_))
          case _             => Left("window: too many arguments")
      yield Ex.Window(f, ps, ks, fr)
    case other => Left(s"unrecognized expression form: ${describe(other)}")

  private def sortKey(sexp: Sexp): Either[String, SortKey] = sexp match
    case Sexp.ListOf(Sexp.Atom("key") :: inner :: Sexp.Atom(dir) :: Sexp.Atom(nulls) :: Nil) =>
      for
        e <- ex(inner)
        d <- dir match
          case "asc" => Right(false); case "desc" => Right(true)
          case other => Left(s"sort direction: $other")
        n <- nulls match
          case "nf" => Right(Some(true)); case "nl" => Right(Some(false)); case "nd" => Right(None)
          case other => Left(s"null ordering: $other")
      yield SortKey(e, d, n)
    case other => Left(s"unrecognized sort key form: ${describe(other)}")

  private def joinType(tag: String): Either[String, JoinType] = tag match
    case "inner" => Right(JoinType.Inner)
    case "full"  => Right(JoinType.FullOuter)
    case "left"  => Right(JoinType.LeftOuter)
    case "right" => Right(JoinType.RightOuter)
    case "anti"  => Right(JoinType.LeftAnti)
    case "semi"  => Right(JoinType.LeftSemi)
    case "cross" => Right(JoinType.Cross)
    case other   => Left(s"unknown join type: $other")

  private def traverse[A](items: List[Sexp])(f: Sexp => Either[String, A]): Either[String, List[A]] =
    items.foldRight[Either[String, List[A]]](Right(Nil)) { (item, acc) =>
      for { a <- f(item); rest <- acc } yield a :: rest
    }

  private def atom(what: String)(sexp: Sexp): Either[String, String] = sexp match
    case Sexp.Atom(v) => Right(dec(v))
    case other        => Left(s"$what must be an atom; got: ${describe(other)}")

  private def schemaCol(sexp: Sexp): Either[String, (String, ColType)] = sexp match
    case Sexp.ListOf(Sexp.Atom("c") :: Sexp.Atom(n) :: Sexp.Atom(t) :: Nil) =>
      parseColType(t).map(dec(n) -> _)
    case other => Left(s"unrecognized schema column form: ${describe(other)}")

  /** One inline-data cell: reuses the literal grammar from [[ex]]. */
  private def litCell(sexp: Sexp): Either[String, LitValue] = ex(sexp).flatMap {
    case Ex.Lit(v) => Right(v)
    case _         => Left(s"localdata cell must be a literal; got: ${describe(sexp)}")
  }

  private def parseFrame(sexp: Sexp): Either[String, WindowFrame] = sexp match
    case Sexp.ListOf(Sexp.Atom("frame") :: Sexp.Atom(kind) :: lower :: upper :: Nil) =>
      for
        rows <- kind match
          case "rows" => Right(true); case "range" => Right(false)
          case other  => Left(s"unknown frame kind: $other")
        l <- parseBoundary(lower)
        u <- parseBoundary(upper)
      yield WindowFrame(rows, l, u)
    case other => Left(s"unrecognized frame form: ${describe(other)}")

  private def parseBoundary(sexp: Sexp): Either[String, FrameBoundary] = sexp match
    case Sexp.ListOf(Sexp.Atom("cur") :: Nil)       => Right(FrameBoundary.CurrentRow)
    case Sexp.ListOf(Sexp.Atom("unbounded") :: Nil) => Right(FrameBoundary.Unbounded)
    case Sexp.ListOf(Sexp.Atom("val") :: e :: Nil)  => ex(e).map(FrameBoundary.Value(_))
    case other => Left(s"unrecognized boundary form: ${describe(other)}")

  private def colTypeTag(t: ColType): String = t match
    case ColType.Bool      => "bool"
    case ColType.I32       => "i32"
    case ColType.I64       => "i64"
    case ColType.F64       => "f64"
    case ColType.Str       => "str"
    case ColType.Timestamp => "ts"
    case ColType.Date      => "date"
    case ColType.Unknown   => "unknown"

  private def parseColType(tag: String): Either[String, ColType] = tag match
    case "bool"    => Right(ColType.Bool)
    case "i32"     => Right(ColType.I32)
    case "i64"     => Right(ColType.I64)
    case "f64"     => Right(ColType.F64)
    case "str"     => Right(ColType.Str)
    case "ts"      => Right(ColType.Timestamp)
    case "date"    => Right(ColType.Date)
    case "unknown" => Right(ColType.Unknown)
    case other     => Left(s"unknown column type tag: $other")

  private def describe(sexp: Sexp): String = sexp match
    case Sexp.Atom(v)     => v
    case Sexp.ListOf(its) => its.take(2).map(describe).mkString("(", " ", " ...)")

  private def enc(s: String): String = LineCodec.enc(s)
  private def dec(s: String): String = LineCodec.dec(s)
