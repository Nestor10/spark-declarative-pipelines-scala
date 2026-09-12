package dev.sdp.core

import dev.sdp.core.algebra.{AlgebraShape, Ex, Rel}

/** A transformation flow that produces the data written to `target`.
  *
  * Flows are first-class fragment content (manifest format v2+): authored
  * bodies travel as canonical [[algebra.RelCodec]] trees (or, for AUTO CDC,
  * as [[FlowDetails.AutoCdc]] parameters) rather than being derived from
  * lineage edges. Lineage *edges* are still derived — from what the flow
  * reads — so build-time cycle/dangling validation sees exactly what the flow
  * actually reads.
  *
  * @param details what the flow does: write a [[algebra.Rel]] (the v2 shape)
  *                or AUTO CDC merge a source (Spark 4.2)
  * @param once    one-time / backfill flow (proto `DefineFlow.once = 8`): the
  *                body runs once and re-runs only on full refresh. `false` is
  *                the v2-compatible default and never widens the manifest.
  */
final case class Flow(
    name: String,
    target: String,
    details: FlowDetails,
    once: Boolean = false,
):

  /** The defining relation, for [[FlowDetails.WriteRelation]] flows. AUTO CDC
    * flows have no defining relation — calling this on one is a defect (the
    * legacy `flow.relation` paths — golden render, manifest-v2 line codec —
    * only ever hold WriteRelation flows). New code must match on `details`. */
  def relation: Rel = details match
    case FlowDetails.WriteRelation(rel) => rel
    case _: FlowDetails.AutoCdc =>
      throw new IllegalStateException(
        s"flow '$name' is an AUTO CDC flow and has no defining relation; match on `details` instead"
      )

object Flow:

  /** Back-compat constructor: a flow that writes `relation` into `target`.
    * Every pre-4.2 call site (`Flow(name, target, rel)`) keeps compiling, and
    * `once` defaults to `false`. */
  def apply(name: String, target: String, relation: Rel): Flow =
    Flow(name, target, FlowDetails.WriteRelation(relation), once = false)

  /** Datasets a *flow* reads — the lineage backstop. AUTO CDC reads exactly
    * its `source`; a relation flow reads via [[reads(Rel)]]. */
  def reads(flow: Flow): Set[String] = flow.details match
    case FlowDetails.WriteRelation(rel) => reads(rel)
    case cdc: FlowDetails.AutoCdc       => Set(cdc.source)

  /** Dataset names this relation reads via `Rel.NamedTable` — including
    * reads inside subquery expressions (lineage sees through `exists`/
    * `scalar`/`in`).
    *
    * `Rel.Sql` bodies are opaque to this collector — SQL references resolve
    * server-side only. That asymmetry is documented author-facing: algebra
    * bodies get compile-time lineage validation, SQL bodies get server-time.
    */
  def reads(relation: Rel): Set[String] =
    allRels(relation).iterator.flatMap { node =>
      val shape = AlgebraShape.of(node)
      shape.datasets ++ shape.exprs.flatMap(exprReads)
    }.toSet

  /** Every relation node in the tree, this one first (structural pre-order;
    * does not descend into subquery expressions — use [[allRelsDeep]] for
    * that). */
  def allRels(relation: Rel): List[Rel] =
    relation :: AlgebraShape.of(relation).children.flatMap(allRels)

  /** Every relation node in the tree INCLUDING the ones embedded in subquery
    * expressions (`exists(...)`, `scalar(...)`, `.in(...)`).
    *
    * This is what a tree-wide GUARD must walk: an inline table hidden in
    * `filter(exists(<huge inline table>))` is exactly as much inline data as a
    * top-level one, and [[allRels]] is blind to it by construction (P3.1).
    *
    * Lineage does NOT need this and must not use it: [[reads]] already sees
    * through subqueries via [[exprReads]], so descending here as well would
    * only re-derive the same names.
    */
  def allRelsDeep(relation: Rel): List[Rel] =
    val shape = AlgebraShape.of(relation)
    relation :: (shape.children ++ shape.exprs.flatMap(subqueryRels)).flatMap(allRelsDeep)

  /** Relations embedded in an expression, at any sub-expression depth. Their
    * own subtrees are not expanded here — [[allRelsDeep]] recurses. */
  private def subqueryRels(ex: Ex): List[Rel] =
    val shape = AlgebraShape.of(ex)
    shape.rels ++ shape.exprs.flatMap(subqueryRels)

  /** Reads hidden inside expressions: subqueries and lambda bodies. */
  private def exprReads(ex: Ex): Set[String] =
    val shape = AlgebraShape.of(ex)
    shape.rels.flatMap(reads).toSet ++ shape.exprs.flatMap(exprReads)
