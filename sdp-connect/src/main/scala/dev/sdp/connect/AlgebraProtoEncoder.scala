package dev.sdp.connect

import scala.jdk.CollectionConverters.*

import dev.sdp.core.algebra.*
import org.apache.spark.connect.proto as sc

/** Pure translation: Tier-0 algebra trees → `spark.connect` protobuf.
  *
  * Field numbers and shapes were read from the canonical
  * `../spark/.../relations.proto` and `expressions.proto`. Every emitted
  * relation case is oracle-verified against a live server's `AnalyzePlan`
  * (see `AlgebraOracleSpec`) before its capability is claimed in
  * `SupportedCapabilities`.
  */
object AlgebraProtoEncoder:

  /** Streaming sources with a fixed, built-in schema that *reject* a
    * user-specified one (Spark errors on `rate().schema(...)`). We keep a
    * declared `.schema` local (inference/codegen) for these but never put it
    * on the wire. */
  private val SchemaRejectingFormats: Set[String] = Set("rate", "socket")

  /** Encoding context: subquery relations are collected as plan-id'd
    * references; a non-empty collection wraps the root in `WithRelations`
    * (the wire's representation of relations-in-expressions).
    */
  private final class Ctx:
    private var nextId: Long = 0
    private val refs         = scala.collection.mutable.ListBuffer.empty[sc.Relation]
    def register(reference: sc.Relation): Long =
      val id = nextId
      nextId += 1
      refs += reference.toBuilder
        .setCommon(reference.getCommon.toBuilder.setPlanId(id))
        .build()
      id
    def references: List[sc.Relation] = refs.toList

  /** Encode a relation tree. Subqueries inside expressions lower to
    * `SubqueryExpression(plan_id)` + `WithRelations` references.
    */
  def relation(rel: Rel): sc.Relation =
    given ctx: Ctx = new Ctx
    val root = go(rel)
    if ctx.references.isEmpty then root
    else
      val w = sc.WithRelations.newBuilder().setRoot(root)
      ctx.references.foreach(w.addReferences)
      sc.Relation.newBuilder().setWithRelations(w).build()

  private def go(rel: Rel)(using ctx: Ctx): sc.Relation =
    rel match
      case Rel.NamedTable(name, streaming) =>
        sc.Relation
          .newBuilder()
          .setRead(
            sc.Read
              .newBuilder()
              .setNamedTable(sc.Read.NamedTable.newBuilder().setUnparsedIdentifier(name))
              .setIsStreaming(streaming)
          )
          .build()

      // `.schema(...)` does double duty: local inference/codegen (valuable for
      // EVERY source, incl. rate) AND the wire schema (file/CSV/JSON sources
      // need it server-side). So we emit the raw `schemaDdl` verbatim —
      // EXCEPT for self-describing streaming sources that *reject* a
      // user-specified schema (`rate`, `socket`): there we keep the declaration
      // local-only. (Spark itself errors on `rate().schema(...)`; suppressing
      // lets authors still declare it for local checks without the wire error.)
      case Rel.DataSource(format, options, streaming, _, schemaDdl) =>
        val source = sc.Read.DataSource.newBuilder().setFormat(format)
        options.toList.sortBy(_._1).foreach((k, v) => source.putOptions(k, v))
        if !SchemaRejectingFormats(format.toLowerCase) then schemaDdl.foreach(source.setSchema)
        sc.Relation
          .newBuilder()
          .setRead(sc.Read.newBuilder().setDataSource(source).setIsStreaming(streaming))
          .build()

      case Rel.Sql(query) =>
        sc.Relation.newBuilder().setSql(sc.SQL.newBuilder().setQuery(query)).build()

      case Rel.Project(input, columns) =>
        sc.Relation
          .newBuilder()
          .setProject(
            sc.Project
              .newBuilder()
              .setInput(go(input))
              .addAllExpressions(columns.map(exGo).asJava)
          )
          .build()

      case Rel.Filter(input, condition) =>
        sc.Relation
          .newBuilder()
          .setFilter(
            sc.Filter
              .newBuilder()
              .setInput(go(input))
              .setCondition(exGo(condition))
          )
          .build()

      case Rel.Join(left, right, condition, joinType) =>
        val join = sc.Join
          .newBuilder()
          .setLeft(go(left))
          .setRight(go(right))
          .setJoinType(protoJoinType(joinType))
        condition.foreach(c => join.setJoinCondition(exGo(c)))
        sc.Relation.newBuilder().setJoin(join).build()

      case Rel.Aggregate(input, groupBy, aggregates) =>
        sc.Relation
          .newBuilder()
          .setAggregate(
            sc.Aggregate
              .newBuilder()
              .setInput(go(input))
              .setGroupType(sc.Aggregate.GroupType.GROUP_TYPE_GROUPBY)
              .addAllGroupingExpressions(groupBy.map(exGo).asJava)
              .addAllAggregateExpressions(aggregates.map(exGo).asJava)
          )
          .build()

      case Rel.Sort(input, order) =>
        sc.Relation
          .newBuilder()
          .setSort(
            sc.Sort
              .newBuilder()
              .setInput(go(input))
              .addAllOrder(order.map(sortOrder).asJava)
              .setIsGlobal(true)
          )
          .build()

      case Rel.Limit(input, n) =>
        sc.Relation
          .newBuilder()
          .setLimit(sc.Limit.newBuilder().setInput(go(input)).setLimit(n))
          .build()

      case Rel.Range(start, end, step) =>
        sc.Relation
          .newBuilder()
          .setRange(sc.Range.newBuilder().setStart(start).setEnd(end).setStep(step))
          .build()

      case Rel.Offset(input, n) =>
        sc.Relation
          .newBuilder()
          .setOffset(sc.Offset.newBuilder().setInput(go(input)).setOffset(n))
          .build()

      case Rel.Tail(input, n) =>
        sc.Relation
          .newBuilder()
          .setTail(sc.Tail.newBuilder().setInput(go(input)).setLimit(n))
          .build()

      case Rel.Deduplicate(input, columns) =>
        val dedup = sc.Deduplicate.newBuilder().setInput(go(input))
        if columns.isEmpty then dedup.setAllColumnsAsKeys(true)
        else columns.foreach(dedup.addColumnNames)
        sc.Relation.newBuilder().setDeduplicate(dedup).build()

      case Rel.Drop(input, columnNames) =>
        val drop = sc.Drop.newBuilder().setInput(go(input))
        columnNames.foreach(drop.addColumnNames)
        sc.Relation.newBuilder().setDrop(drop).build()

      case Rel.SetOp(left, right, op, all) =>
        val opType = op match
          case SetOpType.Union     => sc.SetOperation.SetOpType.SET_OP_TYPE_UNION
          case SetOpType.Intersect => sc.SetOperation.SetOpType.SET_OP_TYPE_INTERSECT
          case SetOpType.Except    => sc.SetOperation.SetOpType.SET_OP_TYPE_EXCEPT
        sc.Relation
          .newBuilder()
          .setSetOp(
            sc.SetOperation
              .newBuilder()
              .setLeftInput(go(left))
              .setRightInput(go(right))
              .setSetOpType(opType)
              .setIsAll(all)
          )
          .build()

      case Rel.SubqueryAlias(input, alias) =>
        sc.Relation
          .newBuilder()
          .setSubqueryAlias(sc.SubqueryAlias.newBuilder().setInput(go(input)).setAlias(alias))
          .build()

      case Rel.ToDF(input, columnNames) =>
        val todf = sc.ToDF.newBuilder().setInput(go(input))
        columnNames.foreach(todf.addColumnNames)
        sc.Relation.newBuilder().setToDf(todf).build()

      case Rel.WithColumns(input, columns) =>
        val wc = sc.WithColumns.newBuilder().setInput(go(input))
        columns.foreach { (name, e) =>
          wc.addAliases(
            sc.Expression.Alias.newBuilder().setExpr(exGo(e)).addName(name)
          )
        }
        sc.Relation.newBuilder().setWithColumns(wc).build()

      case Rel.WithColumnsRenamed(input, renames) =>
        val wcr = sc.WithColumnsRenamed.newBuilder().setInput(go(input))
        renames.foreach { (from, to) =>
          wcr.addRenames(
            sc.WithColumnsRenamed.Rename.newBuilder().setColName(from).setNewColName(to)
          )
        }
        sc.Relation.newBuilder().setWithColumnsRenamed(wcr).build()

      case Rel.Sample(input, fraction, seed) =>
        val sample = sc.Sample
          .newBuilder()
          .setInput(go(input))
          .setLowerBound(0.0)
          .setUpperBound(fraction)
        seed.foreach(sample.setSeed)
        sc.Relation.newBuilder().setSample(sample).build()

      case Rel.Hint(input, name, parameters) =>
        val hint = sc.Hint.newBuilder().setInput(go(input)).setName(name)
        parameters.foreach(p => hint.addParameters(exGo(p)))
        sc.Relation.newBuilder().setHint(hint).build()

      case Rel.Repartition(input, n, shuffle) =>
        sc.Relation
          .newBuilder()
          .setRepartition(
            sc.Repartition.newBuilder().setInput(go(input)).setNumPartitions(n).setShuffle(shuffle)
          )
          .build()

      case Rel.RepartitionByExpression(input, exprs, n) =>
        val r = sc.RepartitionByExpression.newBuilder().setInput(go(input))
        exprs.foreach(e => r.addPartitionExprs(exGo(e)))
        n.foreach(r.setNumPartitions)
        sc.Relation.newBuilder().setRepartitionByExpression(r).build()

      case Rel.DropNa(input, cols) =>
        val d = sc.NADrop.newBuilder().setInput(go(input))
        cols.foreach(d.addCols)
        sc.Relation.newBuilder().setDropNa(d).build()

      case Rel.FillNa(input, cols, value) =>
        val f = sc.NAFill.newBuilder().setInput(go(input))
        cols.foreach(f.addCols)
        f.addValues(literal(value))
        sc.Relation.newBuilder().setFillNa(f).build()

      case Rel.Describe(input, cols) =>
        val d = sc.StatDescribe.newBuilder().setInput(go(input))
        cols.foreach(d.addCols)
        sc.Relation.newBuilder().setDescribe(d).build()

      case Rel.Summary(input, statistics) =>
        val s = sc.StatSummary.newBuilder().setInput(go(input))
        statistics.foreach(s.addStatistics)
        sc.Relation.newBuilder().setSummary(s).build()

      case Rel.Crosstab(input, c1, c2) =>
        sc.Relation
          .newBuilder()
          .setCrosstab(sc.StatCrosstab.newBuilder().setInput(go(input)).setCol1(c1).setCol2(c2))
          .build()

      case Rel.Cov(input, c1, c2) =>
        sc.Relation
          .newBuilder()
          .setCov(sc.StatCov.newBuilder().setInput(go(input)).setCol1(c1).setCol2(c2))
          .build()

      case Rel.Corr(input, c1, c2) =>
        sc.Relation
          .newBuilder()
          .setCorr(sc.StatCorr.newBuilder().setInput(go(input)).setCol1(c1).setCol2(c2))
          .build()

      case Rel.FreqItems(input, cols) =>
        val f = sc.StatFreqItems.newBuilder().setInput(go(input))
        cols.foreach(f.addCols)
        sc.Relation.newBuilder().setFreqItems(f).build()

      case Rel.Unpivot(input, ids, values, varCol, valCol) =>
        val u = sc.Unpivot
          .newBuilder()
          .setInput(go(input))
          .setVariableColumnName(varCol)
          .setValueColumnName(valCol)
        ids.foreach(e => u.addIds(exGo(e)))
        if values.nonEmpty then
          val vs = sc.Unpivot.Values.newBuilder()
          values.foreach(e => vs.addValues(exGo(e)))
          u.setValues(vs)
        sc.Relation.newBuilder().setUnpivot(u).build()

      case Rel.Transpose(input, indexColumns) =>
        val t = sc.Transpose.newBuilder().setInput(go(input))
        indexColumns.foreach(e => t.addIndexColumns(exGo(e)))
        sc.Relation.newBuilder().setTranspose(t).build()

      case Rel.Replace(input, cols, replacements) =>
        val r = sc.NAReplace.newBuilder().setInput(go(input))
        cols.foreach(r.addCols)
        replacements.foreach { (o, n) =>
          r.addReplacements(
            sc.NAReplace.Replacement.newBuilder().setOldValue(literal(o)).setNewValue(literal(n))
          )
        }
        sc.Relation.newBuilder().setReplace(r).build()

      case Rel.SampleBy(input, col, fractions, seed) =>
        val s = sc.StatSampleBy.newBuilder().setInput(go(input)).setCol(exGo(col))
        fractions.foreach { (stratum, fraction) =>
          s.addFractions(
            sc.StatSampleBy.Fraction.newBuilder().setStratum(literal(stratum)).setFraction(fraction)
          )
        }
        seed.foreach(s.setSeed)
        sc.Relation.newBuilder().setSampleBy(s).build()

      case Rel.ApproxQuantile(input, cols, probabilities, relativeError) =>
        val q = sc.StatApproxQuantile.newBuilder().setInput(go(input)).setRelativeError(relativeError)
        cols.foreach(q.addCols)
        probabilities.foreach(p => q.addProbabilities(p))
        sc.Relation.newBuilder().setApproxQuantile(q).build()

      case Rel.CollectMetrics(input, name, metrics) =>
        val m = sc.CollectMetrics.newBuilder().setInput(go(input)).setName(name)
        metrics.foreach(e => m.addMetrics(exGo(e)))
        sc.Relation.newBuilder().setCollectMetrics(m).build()

      case Rel.AsOfJoin(left, right, lk, rk, joinType, direction, exact, tolerance) =>
        val j = sc.AsOfJoin
          .newBuilder()
          .setLeft(go(left))
          .setRight(go(right))
          .setLeftAsOf(exGo(lk))
          .setRightAsOf(exGo(rk))
          .setJoinType(joinType)
          .setDirection(direction)
          .setAllowExactMatches(exact)
        tolerance.foreach(t => j.setTolerance(exGo(t)))
        sc.Relation.newBuilder().setAsOfJoin(j).build()

      case Rel.LateralJoin(left, right, condition, joinType) =>
        val j = sc.LateralJoin
          .newBuilder()
          .setLeft(go(left))
          .setRight(go(right))
          .setJoinType(protoJoinType(joinType))
        condition.foreach(c => j.setJoinCondition(exGo(c)))
        sc.Relation.newBuilder().setLateralJoin(j).build()

      case Rel.Parse(input, format, options) =>
        val fmt = format match
          case ParseFormat.Csv  => sc.Parse.ParseFormat.PARSE_FORMAT_CSV
          case ParseFormat.Json => sc.Parse.ParseFormat.PARSE_FORMAT_JSON
        val p = sc.Parse.newBuilder().setInput(go(input)).setFormat(fmt)
        options.toList.sortBy(_._1).foreach((k, v) => p.putOptions(k, v))
        sc.Relation.newBuilder().setParse(p).build()

      case Rel.ToSchema(input, schema) =>
        sc.Relation
          .newBuilder()
          .setToSchema(sc.ToSchema.newBuilder().setInput(go(input)).setSchema(structType(schema)))
          .build()

      case Rel.LocalRelation(schema) =>
        // schema-only (no Arrow data payload): DDL string per the proto docs
        val ddl = schema.map((n, t) => s"$n ${ddlType(t)}").mkString(", ")
        sc.Relation
          .newBuilder()
          .setLocalRelation(sc.LocalRelation.newBuilder().setSchema(ddl))
          .build()

      case Rel.LocalData(schema, rows) =>
        // D7: the encoder owns the transport — lower inline literals to SQL
        // VALUES (the oracle-verified Sql leaf). Catalyst's EvalInlineTables
        // rebuilds the identical LocalRelation an Arrow payload would. Each
        // cell is CAST to its declared type so the schema is exact (typed
        // NULLs included); the column names come from an `AS t(...)` alias.
        val names = schema.map((n, _) => quoteName(n)).mkString(", ")
        val types = schema.map(_._2)
        val query =
          if rows.isEmpty then
            val nulls = types.map(t => s"CAST(NULL AS ${ddlType(t)})").mkString(", ")
            s"SELECT * FROM (VALUES ($nulls)) AS t($names) WHERE 1 = 0"
          else
            val tuples = rows
              .map(_.zip(types).map((v, t) => s"CAST(${sqlLit(v)} AS ${ddlType(t)})").mkString("(", ", ", ")"))
              .mkString(", ")
            s"SELECT * FROM (VALUES $tuples) AS t($names)"
        sc.Relation.newBuilder().setSql(sc.SQL.newBuilder().setQuery(query)).build()

      case Rel.ShowString(input, numRows, truncate, vertical) =>
        sc.Relation
          .newBuilder()
          .setShowString(
            sc.ShowString
              .newBuilder()
              .setInput(go(input))
              .setNumRows(numRows)
              .setTruncate(truncate)
              .setVertical(vertical)
          )
          .build()

      case Rel.HtmlString(input, numRows, truncate) =>
        sc.Relation
          .newBuilder()
          .setHtmlString(
            sc.HtmlString.newBuilder().setInput(go(input)).setNumRows(numRows).setTruncate(truncate)
          )
          .build()

      case Rel.Tvf(name, args) =>
        val t = sc.UnresolvedTableValuedFunction.newBuilder().setFunctionName(name)
        args.foreach(a => t.addArguments(exGo(a)))
        sc.Relation.newBuilder().setUnresolvedTableValuedFunction(t).build()

      case Rel.Catalog(op) =>
        val cat = sc.Catalog.newBuilder()
        op match
          case CatalogOp.CurrentDatabase =>
            cat.setCurrentDatabase(sc.CurrentDatabase.newBuilder())
          case CatalogOp.ListDatabases =>
            cat.setListDatabases(sc.ListDatabases.newBuilder())
          case CatalogOp.ListTables =>
            cat.setListTables(sc.ListTables.newBuilder())
        sc.Relation.newBuilder().setCatalog(cat).build()

  private def exGo(ex: Ex)(using ctx: Ctx): sc.Expression =
    ex match
      case Ex.Col(name) =>
        sc.Expression
          .newBuilder()
          .setUnresolvedAttribute(
            sc.Expression.UnresolvedAttribute.newBuilder().setUnparsedIdentifier(name)
          )
          .build()

      case Ex.Lit(value) =>
        sc.Expression.newBuilder().setLiteral(literal(value)).build()

      case Ex.Fn(name, args, distinct) =>
        sc.Expression
          .newBuilder()
          .setUnresolvedFunction(
            sc.Expression.UnresolvedFunction
              .newBuilder()
              .setFunctionName(name)
              .addAllArguments(args.map(exGo).asJava)
              .setIsDistinct(distinct)
          )
          .build()

      case Ex.Alias(expr, name) =>
        sc.Expression
          .newBuilder()
          .setAlias(
            sc.Expression.Alias.newBuilder().setExpr(exGo(expr)).addName(name)
          )
          .build()

      case Ex.Cast(expr, to) =>
        sc.Expression
          .newBuilder()
          .setCast(
            sc.Expression.Cast
              .newBuilder()
              .setExpr(exGo(expr))
              .setTypeStr(ddlType(to))
              .setEvalMode(sc.Expression.Cast.EvalMode.EVAL_MODE_ANSI)
          )
          .build()

      case Ex.ExprString(sql) =>
        sc.Expression
          .newBuilder()
          .setExpressionString(sc.Expression.ExpressionString.newBuilder().setExpression(sql))
          .build()

      case Ex.Star(target) =>
        val star = sc.Expression.UnresolvedStar.newBuilder()
        target.foreach(star.setUnparsedTarget)
        sc.Expression.newBuilder().setUnresolvedStar(star).build()

      case Ex.ExtractValue(child, extraction) =>
        sc.Expression
          .newBuilder()
          .setUnresolvedExtractValue(
            sc.Expression.UnresolvedExtractValue
              .newBuilder()
              .setChild(exGo(child))
              .setExtraction(exGo(extraction))
          )
          .build()

      case Ex.ColRegex(pattern) =>
        sc.Expression
          .newBuilder()
          .setUnresolvedRegex(sc.Expression.UnresolvedRegex.newBuilder().setColName(pattern))
          .build()

      case Ex.CallFn(name, args) =>
        val call = sc.CallFunction.newBuilder().setFunctionName(name)
        args.foreach(a => call.addArguments(exGo(a)))
        sc.Expression.newBuilder().setCallFunction(call).build()

      case Ex.Window(function, partitionBy, orderBy, frame) =>
        val w = sc.Expression.Window.newBuilder().setWindowFunction(exGo(function))
        partitionBy.foreach(p => w.addPartitionSpec(exGo(p)))
        orderBy.foreach(k => w.addOrderSpec(sortOrder(k)))
        frame.foreach(f => w.setFrameSpec(windowFrame(f)))
        sc.Expression.newBuilder().setWindow(w).build()

      case Ex.Lam(params, body) =>
        val lam = sc.Expression.LambdaFunction.newBuilder().setFunction(exGo(body))
        params.foreach { p =>
          lam.addArguments(
            sc.Expression.UnresolvedNamedLambdaVariable.newBuilder().addNameParts(p)
          )
        }
        sc.Expression.newBuilder().setLambdaFunction(lam).build()

      case Ex.LamVar(name) =>
        sc.Expression
          .newBuilder()
          .setUnresolvedNamedLambdaVariable(
            sc.Expression.UnresolvedNamedLambdaVariable.newBuilder().addNameParts(name)
          )
          .build()

      case Ex.Subquery(rel, kind) =>
        val planId = ctx.register(go(rel))
        val subq = sc.SubqueryExpression.newBuilder().setPlanId(planId)
        kind match
          case SubqueryKind.Scalar =>
            subq.setSubqueryType(sc.SubqueryExpression.SubqueryType.SUBQUERY_TYPE_SCALAR)
          case SubqueryKind.Exists =>
            subq.setSubqueryType(sc.SubqueryExpression.SubqueryType.SUBQUERY_TYPE_EXISTS)
          case SubqueryKind.In(values) =>
            subq.setSubqueryType(sc.SubqueryExpression.SubqueryType.SUBQUERY_TYPE_IN)
            values.foreach(v => subq.addInSubqueryValues(exGo(v)))
        sc.Expression.newBuilder().setSubqueryExpression(subq).build()

  private def literal(value: LitValue): sc.Expression.Literal =
    val b = sc.Expression.Literal.newBuilder()
    value match
      case LitValue.Bool(v) => b.setBoolean(v)
      case LitValue.I32(v)  => b.setInteger(v)
      case LitValue.I64(v)  => b.setLong(v)
      case LitValue.F64(v)  => b.setDouble(v)
      case LitValue.Str(v)  => b.setString(v)
      case LitValue.Null    =>
        b.setNull(sc.DataType.newBuilder().setNull(sc.DataType.NULL.newBuilder()).build())
    b.build()

  private def sortOrder(key: SortKey)(using Ctx): sc.Expression.SortOrder =
    val b = sc.Expression.SortOrder
      .newBuilder()
      .setChild(exGo(key.expr))
      .setDirection(
        if key.descending then sc.Expression.SortOrder.SortDirection.SORT_DIRECTION_DESCENDING
        else sc.Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING
      )
    key.nullsFirst.foreach { first =>
      val _ = b.setNullOrdering(
        if first then sc.Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST
        else sc.Expression.SortOrder.NullOrdering.SORT_NULLS_LAST
      )
    }
    b.build()

  private def windowFrame(f: WindowFrame)(using Ctx): sc.Expression.Window.WindowFrame =
    sc.Expression.Window.WindowFrame
      .newBuilder()
      .setFrameType(
        if f.rowFrame then sc.Expression.Window.WindowFrame.FrameType.FRAME_TYPE_ROW
        else sc.Expression.Window.WindowFrame.FrameType.FRAME_TYPE_RANGE
      )
      .setLower(frameBoundary(f.lower))
      .setUpper(frameBoundary(f.upper))
      .build()

  private def frameBoundary(b: FrameBoundary)(using Ctx): sc.Expression.Window.WindowFrame.FrameBoundary =
    val fb = sc.Expression.Window.WindowFrame.FrameBoundary.newBuilder()
    b match
      case FrameBoundary.CurrentRow  => fb.setCurrentRow(true)
      case FrameBoundary.Unbounded   => fb.setUnbounded(true)
      case FrameBoundary.Value(expr) => fb.setValue(exGo(expr))
    fb.build()

  /** Build a struct `DataType` from declared columns (for `ToSchema`). */
  private def structType(schema: List[(String, ColType)]): sc.DataType =
    val struct = sc.DataType.Struct.newBuilder()
    schema.foreach { (name, t) =>
      struct.addFields(
        sc.DataType.StructField
          .newBuilder()
          .setName(name)
          .setDataType(dataType(t))
          .setNullable(true)
      )
    }
    sc.DataType.newBuilder().setStruct(struct).build()

  private def dataType(t: ColType): sc.DataType =
    val b = sc.DataType.newBuilder()
    t match
      case ColType.Bool      => b.setBoolean(sc.DataType.Boolean.newBuilder())
      case ColType.I32       => b.setInteger(sc.DataType.Integer.newBuilder())
      case ColType.I64       => b.setLong(sc.DataType.Long.newBuilder())
      case ColType.F64       => b.setDouble(sc.DataType.Double.newBuilder())
      case ColType.Str       => b.setString(sc.DataType.String.newBuilder())
      case ColType.Timestamp => b.setTimestamp(sc.DataType.Timestamp.newBuilder())
      case ColType.Date      => b.setDate(sc.DataType.Date.newBuilder())
      case ColType.Unknown =>
        throw new IllegalArgumentException("ColType.Unknown is not encodable as a wire DataType")
    b.build()

  /** ColType → Spark DDL type name (for `Cast.type_str`). */
  /** Render a literal as a SQL constant for inline-data `VALUES` lowering.
    * Strings are single-quoted with `'` doubled; the CAST around the call site
    * fixes the type, so numeric forms are plain. */
  private def sqlLit(v: dev.sdp.core.algebra.LitValue): String =
    import dev.sdp.core.algebra.LitValue
    v match
      case LitValue.Bool(b) => b.toString
      case LitValue.I32(i)  => i.toString
      case LitValue.I64(l)  => l.toString
      case LitValue.F64(d)  => d.toString
      case LitValue.Str(s)  => "'" + s.replace("'", "''") + "'"
      case LitValue.Null    => "NULL"

  /** Backtick-quote a column identifier for the `AS t(...)` alias. */
  private def quoteName(name: String): String = "`" + name.replace("`", "``") + "`"

  private def ddlType(t: ColType): String = t match
    case ColType.Bool      => "BOOLEAN"
    case ColType.I32       => "INT"
    case ColType.I64       => "BIGINT"
    case ColType.F64       => "DOUBLE"
    case ColType.Str       => "STRING"
    case ColType.Timestamp => "TIMESTAMP"
    case ColType.Date      => "DATE"
    case ColType.Unknown =>
      // unreachable from the DSL (tokens are concrete); a programmatic tree
      // with an Unknown cast is a construction bug, not author error
      throw new IllegalArgumentException("Cast to ColType.Unknown is not encodable")

  private def protoJoinType(jt: JoinType): sc.Join.JoinType =
    jt match
      case JoinType.Inner      => sc.Join.JoinType.JOIN_TYPE_INNER
      case JoinType.FullOuter  => sc.Join.JoinType.JOIN_TYPE_FULL_OUTER
      case JoinType.LeftOuter  => sc.Join.JoinType.JOIN_TYPE_LEFT_OUTER
      case JoinType.RightOuter => sc.Join.JoinType.JOIN_TYPE_RIGHT_OUTER
      case JoinType.LeftAnti   => sc.Join.JoinType.JOIN_TYPE_LEFT_ANTI
      case JoinType.LeftSemi   => sc.Join.JoinType.JOIN_TYPE_LEFT_SEMI
      case JoinType.Cross      => sc.Join.JoinType.JOIN_TYPE_CROSS
