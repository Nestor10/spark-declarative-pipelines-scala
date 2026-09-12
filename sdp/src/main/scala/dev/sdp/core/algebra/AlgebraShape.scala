package dev.sdp.core.algebra

/** The structural view of ONE algebra node: what it names, what it contains.
  *
  * This is the single source of truth for every *structural* traversal of the
  * algebra — lineage (`Flow.reads`), the tree-wide guards
  * (`InlineDataGuard`), and anything else that needs "walk the tree" rather
  * than "interpret the tree".
  *
  * Why it exists (P3.1): those traversals used to be three parallel ~45-case
  * matches in `Flow` (`children`, `exprsOf`, `structuralReads`), two of which
  * ended in `case _ => Nil`. A new `Rel` case therefore compiled cleanly and
  * silently dropped its subtree — no lineage edges, false-negative
  * cycle/dangling validation, and a size guard that never saw the data. Now
  * there is exactly one match over `Rel` here; it is exhaustive, and
  * `-Wconf:msg=match may not be exhaustive:e` (build.sbt) makes a missing case
  * a compile ERROR rather than a warning. Adding a `Rel` case fails
  * compilation here, in `RelCodec.render`, in `SchemaCheck.infer` and in
  * `AlgebraProtoEncoder` — the four places that must have an opinion.
  *
  * Semantic traversals (`RelCodec.render`, `SchemaCheck.infer`, the proto
  * encoder) deliberately keep their own matches: each node means something
  * different to them, so there is nothing to share but the obligation to be
  * total, which the compiler now enforces.
  *
  * @param datasets dataset names this node *itself* reads (only `NamedTable`
  *                 does; `Sql` bodies are opaque by design — their references
  *                 resolve server-side)
  * @param children immediate child relations
  * @param exprs    expressions held directly by this node (one level)
  */
final case class RelShape(
    datasets: List[String] = Nil,
    children: List[Rel] = Nil,
    exprs: List[Ex] = Nil,
)

/** The same, for one expression node: relations it embeds (subqueries) and
  * its immediate sub-expressions. */
final case class ExShape(
    rels: List[Rel] = Nil,
    exprs: List[Ex] = Nil,
)

object AlgebraShape:

  /** The one exhaustive match over `Rel`. Leaves declare themselves by
    * returning an empty shape — explicitly, never by falling into a
    * catch-all. */
  def of(rel: Rel): RelShape = rel match
    // --- leaves: introduce data (or metadata), read no dataset ---------
    case Rel.NamedTable(name, _)       => RelShape(datasets = List(name))
    case Rel.DataSource(_, _, _, _, _) => RelShape() // external — introduces data
    case Rel.Sql(_)                    => RelShape() // opaque: refs resolve server-side
    case Rel.Range(_, _, _)            => RelShape() // generated
    case Rel.LocalRelation(_)          => RelShape() // typed leaf
    case Rel.LocalData(_, _)           => RelShape() // inline literals
    case Rel.Catalog(_)                => RelShape() // metadata
    case Rel.Tvf(_, args)              => RelShape(exprs = args) // generated, but args are exprs

    // --- one input, no expressions -------------------------------------
    case Rel.Limit(i, _)                 => RelShape(children = List(i))
    case Rel.Offset(i, _)                => RelShape(children = List(i))
    case Rel.Tail(i, _)                  => RelShape(children = List(i))
    case Rel.Deduplicate(i, _)           => RelShape(children = List(i))
    case Rel.Drop(i, _)                  => RelShape(children = List(i))
    case Rel.SubqueryAlias(i, _)         => RelShape(children = List(i))
    case Rel.ToDF(i, _)                  => RelShape(children = List(i))
    case Rel.WithColumnsRenamed(i, _)    => RelShape(children = List(i))
    case Rel.Sample(i, _, _)             => RelShape(children = List(i))
    case Rel.Repartition(i, _, _)        => RelShape(children = List(i))
    case Rel.DropNa(i, _)                => RelShape(children = List(i))
    case Rel.FillNa(i, _, _)             => RelShape(children = List(i))
    case Rel.Replace(i, _, _)            => RelShape(children = List(i))
    case Rel.ApproxQuantile(i, _, _, _)  => RelShape(children = List(i))
    case Rel.Parse(i, _, _)              => RelShape(children = List(i))
    case Rel.ToSchema(i, _)              => RelShape(children = List(i))
    case Rel.ShowString(i, _, _, _)      => RelShape(children = List(i))
    case Rel.HtmlString(i, _, _)         => RelShape(children = List(i))
    case Rel.Describe(i, _)              => RelShape(children = List(i))
    case Rel.Summary(i, _)               => RelShape(children = List(i))
    case Rel.Crosstab(i, _, _)           => RelShape(children = List(i))
    case Rel.Cov(i, _, _)                => RelShape(children = List(i))
    case Rel.Corr(i, _, _)               => RelShape(children = List(i))
    case Rel.FreqItems(i, _)             => RelShape(children = List(i))

    // --- one input, expressions ----------------------------------------
    case Rel.Project(i, columns)     => RelShape(children = List(i), exprs = columns)
    case Rel.Filter(i, condition)    => RelShape(children = List(i), exprs = List(condition))
    case Rel.Aggregate(i, gs, as)    => RelShape(children = List(i), exprs = gs ++ as)
    case Rel.Sort(i, order)          => RelShape(children = List(i), exprs = order.map(_.expr))
    case Rel.WithColumns(i, columns) => RelShape(children = List(i), exprs = columns.map(_._2))
    case Rel.Hint(i, _, parameters)  => RelShape(children = List(i), exprs = parameters)
    case Rel.RepartitionByExpression(i, es, _) => RelShape(children = List(i), exprs = es)
    case Rel.Unpivot(i, ids, values, _, _)     => RelShape(children = List(i), exprs = ids ++ values)
    case Rel.Transpose(i, indexColumns)        => RelShape(children = List(i), exprs = indexColumns)
    case Rel.SampleBy(i, col, _, _)            => RelShape(children = List(i), exprs = List(col))
    case Rel.CollectMetrics(i, _, metrics)     => RelShape(children = List(i), exprs = metrics)

    // --- two inputs ------------------------------------------------------
    case Rel.Join(l, r, condition, _)        => RelShape(children = List(l, r), exprs = condition.toList)
    case Rel.SetOp(l, r, _, _)               => RelShape(children = List(l, r))
    case Rel.LateralJoin(l, r, condition, _) => RelShape(children = List(l, r), exprs = condition.toList)
    case Rel.AsOfJoin(l, r, lk, rk, _, _, _, tolerance) =>
      RelShape(children = List(l, r), exprs = List(lk, rk) ++ tolerance.toList)

  /** The one exhaustive match over `Ex`. */
  def of(ex: Ex): ExShape = ex match
    case Ex.Col(_)         => ExShape()
    case Ex.Lit(_)         => ExShape()
    case Ex.ExprString(_)  => ExShape() // opaque SQL — server resolves
    case Ex.Star(_)        => ExShape()
    case Ex.ColRegex(_)    => ExShape()
    case Ex.LamVar(_)      => ExShape() // bound by the enclosing Lam
    case Ex.Fn(_, args, _) => ExShape(exprs = args)
    case Ex.CallFn(_, args) => ExShape(exprs = args)
    case Ex.Alias(e, _)    => ExShape(exprs = List(e))
    case Ex.Cast(e, _)     => ExShape(exprs = List(e))
    case Ex.Lam(_, body)   => ExShape(exprs = List(body))
    case Ex.ExtractValue(child, extraction) => ExShape(exprs = List(child, extraction))
    case Ex.Window(function, partitionBy, orderBy, frame) =>
      ExShape(exprs =
        (function :: partitionBy) ++ orderBy.map(_.expr) ++ frame.toList.flatMap(frameExprs)
      )
    case Ex.Subquery(rel, kind) =>
      val values = kind match
        case SubqueryKind.In(vs)                     => vs
        case SubqueryKind.Scalar | SubqueryKind.Exists => Nil
      ExShape(rels = List(rel), exprs = values)

  private def frameExprs(frame: WindowFrame): List[Ex] =
    boundaryExprs(frame.lower) ++ boundaryExprs(frame.upper)

  private def boundaryExprs(b: FrameBoundary): List[Ex] = b match
    case FrameBoundary.CurrentRow  => Nil
    case FrameBoundary.Unbounded   => Nil
    case FrameBoundary.Value(expr) => List(expr)
