package dev.sdp.core

import dev.sdp.core.algebra.{Ex, Rel, SubqueryKind}

/** A transformation flow: `relation` computes the data written to `target`.
  *
  * Flows are first-class fragment content (manifest format v2): authored
  * bodies travel as canonical [[algebra.RelCodec]] trees rather than being
  * derived from lineage edges. Lineage *edges* are still derived — from the
  * relation's reads — so compile-time cycle/dangling validation sees exactly
  * what the flow actually reads.
  */
final case class Flow(name: String, target: String, relation: Rel)

object Flow:

  /** Dataset names this relation reads via `Rel.NamedTable` — including
    * reads inside subquery expressions (lineage sees through `exists`/
    * `scalar`/`in`).
    *
    * `Rel.Sql` bodies are opaque to this collector — SQL references resolve
    * server-side only. That asymmetry is documented author-facing: algebra
    * bodies get compile-time lineage validation, SQL bodies get server-time.
    */
  def reads(relation: Rel): Set[String] =
    structuralReads(relation) ++ exprsOf(relation).flatMap(exprReads).toSet

  /** Every relation node in the tree, this one first (structural pre-order;
    * does not descend into subquery expressions). Used by tree-wide checks
    * like the inline-data size guard. */
  def allRels(relation: Rel): List[Rel] =
    relation :: children(relation).flatMap(allRels)

  /** Immediate child relations. Catch-all `Nil` covers every leaf
    * (NamedTable/DataSource/Sql/Range/LocalRelation/LocalData/Tvf/Catalog). */
  private def children(relation: Rel): List[Rel] = relation match
    case Rel.Project(i, _)                        => List(i)
    case Rel.Filter(i, _)                         => List(i)
    case Rel.Join(l, r, _, _)                     => List(l, r)
    case Rel.Aggregate(i, _, _)                   => List(i)
    case Rel.Sort(i, _)                           => List(i)
    case Rel.Limit(i, _)                          => List(i)
    case Rel.Offset(i, _)                         => List(i)
    case Rel.Tail(i, _)                           => List(i)
    case Rel.Deduplicate(i, _)                    => List(i)
    case Rel.Drop(i, _)                           => List(i)
    case Rel.SetOp(l, r, _, _)                    => List(l, r)
    case Rel.SubqueryAlias(i, _)                  => List(i)
    case Rel.ToDF(i, _)                           => List(i)
    case Rel.WithColumns(i, _)                    => List(i)
    case Rel.WithColumnsRenamed(i, _)             => List(i)
    case Rel.Sample(i, _, _)                      => List(i)
    case Rel.Hint(i, _, _)                        => List(i)
    case Rel.Repartition(i, _, _)                 => List(i)
    case Rel.RepartitionByExpression(i, _, _)     => List(i)
    case Rel.DropNa(i, _)                         => List(i)
    case Rel.FillNa(i, _, _)                      => List(i)
    case Rel.AsOfJoin(l, r, _, _, _, _, _, _)     => List(l, r)
    case Rel.LateralJoin(l, r, _, _)              => List(l, r)
    case Rel.Parse(i, _, _)                       => List(i)
    case Rel.ToSchema(i, _)                       => List(i)
    case Rel.ShowString(i, _, _, _)               => List(i)
    case Rel.HtmlString(i, _, _)                  => List(i)
    case Rel.Unpivot(i, _, _, _, _)               => List(i)
    case Rel.Transpose(i, _)                      => List(i)
    case Rel.Replace(i, _, _)                     => List(i)
    case Rel.SampleBy(i, _, _, _)                 => List(i)
    case Rel.ApproxQuantile(i, _, _, _)           => List(i)
    case Rel.CollectMetrics(i, _, _)              => List(i)
    case Rel.Describe(i, _)                       => List(i)
    case Rel.Summary(i, _)                        => List(i)
    case Rel.Crosstab(i, _, _)                    => List(i)
    case Rel.Cov(i, _, _)                         => List(i)
    case Rel.Corr(i, _, _)                        => List(i)
    case Rel.FreqItems(i, _)                      => List(i)
    case _                                        => Nil

  /** Reads hidden inside expressions: subqueries and lambda bodies. */
  private def exprReads(ex: Ex): Set[String] = ex match
    case Ex.Subquery(rel, kind) =>
      val valueReads = kind match
        case SubqueryKind.In(values) => values.flatMap(exprReads).toSet
        case _                       => Set.empty[String]
      reads(rel) ++ valueReads
    case Ex.Lam(_, body)           => exprReads(body)
    case Ex.Fn(_, args, _)         => args.flatMap(exprReads).toSet
    case Ex.CallFn(_, args)        => args.flatMap(exprReads).toSet
    case Ex.Alias(e, _)            => exprReads(e)
    case Ex.Cast(e, _)             => exprReads(e)
    case Ex.ExtractValue(c, e)     => exprReads(c) ++ exprReads(e)
    case Ex.Window(f, parts, keys, _) =>
      exprReads(f) ++ parts.flatMap(exprReads) ++ keys.flatMap(k => exprReads(k.expr))
    case _ => Set.empty

  /** Expressions held directly by a relation node (one level). */
  private def exprsOf(relation: Rel): List[Ex] = relation match
    case Rel.Project(_, columns)              => columns
    case Rel.Filter(_, condition)             => List(condition)
    case Rel.Join(_, _, condition, _)         => condition.toList
    case Rel.Aggregate(_, groupBy, aggs)      => groupBy ++ aggs
    case Rel.Sort(_, order)                   => order.map(_.expr)
    case Rel.WithColumns(_, columns)          => columns.map(_._2)
    case Rel.Hint(_, _, parameters)           => parameters
    case Rel.RepartitionByExpression(_, es, _) => es
    case Rel.Unpivot(_, ids, values, _, _)    => ids ++ values
    case Rel.Transpose(_, idx)                => idx
    case Rel.SampleBy(_, col, _, _)           => List(col)
    case Rel.CollectMetrics(_, _, metrics)    => metrics
    case Rel.AsOfJoin(_, _, lk, rk, _, _, _, tol) => List(lk, rk) ++ tol.toList
    case Rel.LateralJoin(_, _, condition, _)  => condition.toList
    case Rel.Tvf(_, args)                     => args
    case _                                    => Nil

  private def structuralReads(relation: Rel): Set[String] = relation match
    case Rel.NamedTable(name, _)          => Set(name)
    case Rel.DataSource(_, _, _, _, _)    => Set.empty // external — introduces data, reads no dataset
    case Rel.Sql(_)                       => Set.empty
    case Rel.Range(_, _, _)               => Set.empty // generated — reads no dataset
    case Rel.Project(input, _)            => reads(input)
    case Rel.Filter(input, _)             => reads(input)
    case Rel.Join(left, right, _, _)      => reads(left) ++ reads(right)
    case Rel.Aggregate(input, _, _)       => reads(input)
    case Rel.Sort(input, _)               => reads(input)
    case Rel.Limit(input, _)              => reads(input)
    case Rel.Offset(input, _)             => reads(input)
    case Rel.Tail(input, _)               => reads(input)
    case Rel.Deduplicate(input, _)        => reads(input)
    case Rel.Drop(input, _)               => reads(input)
    case Rel.SetOp(left, right, _, _)     => reads(left) ++ reads(right)
    case Rel.SubqueryAlias(input, _)      => reads(input)
    case Rel.ToDF(input, _)               => reads(input)
    case Rel.WithColumns(input, _)        => reads(input)
    case Rel.WithColumnsRenamed(input, _) => reads(input)
    case Rel.Sample(input, _, _)          => reads(input)
    case Rel.Hint(input, _, _)            => reads(input)
    case Rel.Repartition(input, _, _)     => reads(input)
    case Rel.RepartitionByExpression(input, _, _) => reads(input)
    case Rel.DropNa(input, _)             => reads(input)
    case Rel.FillNa(input, _, _)          => reads(input)
    case Rel.AsOfJoin(l, r, _, _, _, _, _, _) => reads(l) ++ reads(r)
    case Rel.LateralJoin(l, r, _, _)      => reads(l) ++ reads(r)
    case Rel.Parse(input, _, _)           => reads(input)
    case Rel.ToSchema(input, _)           => reads(input)
    case Rel.LocalRelation(_)             => Set.empty // typed leaf — introduces nothing read
    case Rel.LocalData(_, _)              => Set.empty // inline literals — introduces data, reads no dataset
    case Rel.ShowString(input, _, _, _)   => reads(input)
    case Rel.HtmlString(input, _, _)      => reads(input)
    case Rel.Tvf(_, _)                    => Set.empty // generated — reads no dataset
    case Rel.Catalog(_)                   => Set.empty // metadata — reads no dataset
    case Rel.Unpivot(input, _, _, _, _)   => reads(input)
    case Rel.Transpose(input, _)          => reads(input)
    case Rel.Replace(input, _, _)         => reads(input)
    case Rel.SampleBy(input, _, _, _)     => reads(input)
    case Rel.ApproxQuantile(input, _, _, _) => reads(input)
    case Rel.CollectMetrics(input, _, _)  => reads(input)
    case Rel.Describe(input, _)           => reads(input)
    case Rel.Summary(input, _)            => reads(input)
    case Rel.Crosstab(input, _, _)        => reads(input)
    case Rel.Cov(input, _, _)             => reads(input)
    case Rel.Corr(input, _, _)            => reads(input)
    case Rel.FreqItems(input, _)          => reads(input)
