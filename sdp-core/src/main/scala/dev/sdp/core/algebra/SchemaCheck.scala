package dev.sdp.core.algebra

/** Gradual, build-time column-existence checking.
  *
  * Schemas are declared only where they are unknowable — external source
  * leaves — and *inferred* everywhere else by propagating column sets
  * through the relation algebra. Datasets whose schema cannot be known
  * (SQL bodies, undeclared sources) are `Unknown`, and everything
  * downstream of them is simply unchecked: gradual means never wrong,
  * never ceremonial.
  *
  * v1 checks column **existence** — the highest-value failure (typos,
  * dropped-column references) with none of the cost of reimplementing
  * Catalyst's type system. Declared `ColType`s travel for CAST and future
  * wire emission, but `fn(...)` results are typed `Unknown` on purpose.
  */
/** Column types for declared schemas. `Unknown` marks inferred columns whose
  * type the checker doesn't track (e.g. `fn(...)` results) — existence is
  * still checked.
  */
enum ColType:
  case Bool, I32, I64, F64, Str, Timestamp, Date
  case Unknown

object SchemaCheck:

  /** A dataset's inferred or declared shape. */
  enum Shape:
    case Known(columns: List[(String, ColType)])
    case Unknown

    def columnNames: Option[List[String]] = this match
      case Known(cols) => Some(cols.map(_._1))
      case Unknown     => None

  enum SchemaError:
    case UnknownColumn(column: String, available: List[String])

  /** Infer the output shape of `rel`, given upstream dataset shapes,
    * accumulating every column-existence error found along the way.
    */
  def infer(rel: Rel, upstream: String => Shape): (List[SchemaError], Shape) =
    rel match
      case Rel.NamedTable(name, _) => (Nil, upstream(name))

      case Rel.DataSource(_, _, _, schema, _) =>
        (Nil, if schema.isEmpty then Shape.Unknown else Shape.Known(schema))
      case Rel.Sql(_)         => (Nil, Shape.Unknown)
      case Rel.Range(_, _, _) => (Nil, Shape.Known(List("id" -> ColType.I64)))

      case Rel.Project(input, columns) =>
        val (errs, in)     = infer(input, upstream)
        val (refErrs, out) = projectShape(in, columns)
        (errs ++ refErrs, out)

      case Rel.Filter(input, condition) =>
        val (errs, in) = infer(input, upstream)
        (errs ++ refErrors(in, List(condition)), in)

      case Rel.Join(left, right, condition, joinType) =>
        val (lErrs, l) = infer(left, upstream)
        val (rErrs, r) = infer(right, upstream)
        val merged = (l, r) match
          case (Shape.Known(lc), Shape.Known(rc)) =>
            joinType match
              case JoinType.LeftSemi | JoinType.LeftAnti => Shape.Known(lc)
              case _                                     => Shape.Known(lc ++ rc)
          case _ => Shape.Unknown
        val condErrs = condition.toList.flatMap(c => refErrors(merged, List(c)))
        (lErrs ++ rErrs ++ condErrs, merged)

      case Rel.Aggregate(input, groupBy, aggregates) =>
        val (errs, in) = infer(input, upstream)
        val refErrs    = refErrors(in, groupBy ++ aggregates)
        val out = in match
          case Shape.Unknown => Shape.Unknown
          case Shape.Known(inCols) =>
            Shape.Known((groupBy ++ aggregates).flatMap(outputColumn(_, inCols)))
        (errs ++ refErrs, out)

      case Rel.Sort(input, order) =>
        val (errs, in) = infer(input, upstream)
        (errs ++ refErrors(in, order.map(_.expr)), in)

      case Rel.Limit(input, _)  => infer(input, upstream)
      case Rel.Offset(input, _) => infer(input, upstream)
      case Rel.Tail(input, _)   => infer(input, upstream)

      case Rel.Deduplicate(input, columns) =>
        val (errs, in) = infer(input, upstream)
        (errs ++ nameErrors(in, columns), in)

      case Rel.Drop(input, columnNames) =>
        val (errs, in) = infer(input, upstream)
        val out = in match
          case Shape.Known(cols) => Shape.Known(cols.filterNot((n, _) => columnNames.contains(n)))
          case Shape.Unknown     => Shape.Unknown
        (errs ++ nameErrors(in, columnNames), out)

      case Rel.SetOp(left, right, _, _) =>
        val (lErrs, l) = infer(left, upstream)
        val (rErrs, _) = infer(right, upstream)
        (lErrs ++ rErrs, l) // SQL semantics: left side names the output

      case Rel.SubqueryAlias(input, _) => infer(input, upstream)

      case Rel.Sample(input, _, _) => infer(input, upstream)

      case Rel.Hint(input, _, parameters) =>
        val (errs, in) = infer(input, upstream)
        (errs ++ refErrors(in, parameters), in)

      case Rel.Repartition(input, _, _) => infer(input, upstream)

      case Rel.RepartitionByExpression(input, exprs, _) =>
        val (errs, in) = infer(input, upstream)
        (errs ++ refErrors(in, exprs), in)

      case Rel.DropNa(input, cols) =>
        val (errs, in) = infer(input, upstream)
        (errs ++ nameErrors(in, cols), in)

      case Rel.FillNa(input, cols, _) =>
        val (errs, in) = infer(input, upstream)
        (errs ++ nameErrors(in, cols), in)

      case Rel.AsOfJoin(left, right, leftAsOf, rightAsOf, _, _, _, tolerance) =>
        val (lErrs, l) = infer(left, upstream)
        val (rErrs, r) = infer(right, upstream)
        val merged = (l, r) match
          case (Shape.Known(lc), Shape.Known(rc)) => Shape.Known(lc ++ rc)
          case _                                  => Shape.Unknown
        val keyErrs = refErrors(l, List(leftAsOf)) ++ refErrors(r, List(rightAsOf)) ++
          tolerance.toList.flatMap(t => refErrors(merged, List(t)))
        (lErrs ++ rErrs ++ keyErrs, merged)

      case Rel.LateralJoin(left, right, condition, _) =>
        val (lErrs, l) = infer(left, upstream)
        val (rErrs, r) = infer(right, upstream)
        val merged = (l, r) match
          case (Shape.Known(lc), Shape.Known(rc)) => Shape.Known(lc ++ rc)
          case _                                  => Shape.Unknown
        (lErrs ++ rErrs ++ condition.toList.flatMap(c => refErrors(merged, List(c))), merged)

      case Rel.Parse(input, _, _) =>
        (infer(input, upstream)._1, Shape.Unknown) // schema inferred server-side

      case Rel.ToSchema(input, schema) =>
        (infer(input, upstream)._1, Shape.Known(schema)) // declared waypoint

      case Rel.LocalRelation(schema) => (Nil, Shape.Known(schema)) // typed leaf
      case Rel.LocalData(schema, _)  => (Nil, Shape.Known(schema)) // inline literals — schema declared

      case Rel.ShowString(input, _, _, _) => (infer(input, upstream)._1, Shape.Unknown)
      case Rel.HtmlString(input, _, _)    => (infer(input, upstream)._1, Shape.Unknown)
      case Rel.Tvf(_, _)                  => (Nil, Shape.Unknown)
      case Rel.Catalog(_)                 => (Nil, Shape.Unknown)

      case Rel.Unpivot(input, ids, values, variableCol, valueCol) =>
        val (errs, in) = infer(input, upstream)
        val refErrs    = refErrors(in, ids ++ values)
        val out = in match
          case Shape.Unknown => Shape.Unknown
          case Shape.Known(inCols) =>
            val idCols = ids.flatMap(outputColumn(_, inCols))
            if idCols.sizeIs == ids.size then
              Shape.Known(idCols ++ List(variableCol -> ColType.Str, valueCol -> ColType.Unknown))
            else Shape.Unknown
        (errs ++ refErrs, out)

      case Rel.Transpose(input, indexColumns) =>
        val (errs, in) = infer(input, upstream)
        (errs ++ refErrors(in, indexColumns), Shape.Unknown) // shape is data-dependent

      case Rel.Replace(input, cols, _) =>
        val (errs, in) = infer(input, upstream)
        (errs ++ nameErrors(in, cols), in)

      case Rel.SampleBy(input, col, _, _) =>
        val (errs, in) = infer(input, upstream)
        (errs ++ refErrors(in, List(col)), in)

      case Rel.ApproxQuantile(input, cols, _, _) =>
        val (errs, in) = infer(input, upstream)
        (errs ++ nameErrors(in, cols), Shape.Unknown) // array-of-double schema, server-shaped

      case Rel.CollectMetrics(input, _, metrics) =>
        val (errs, in) = infer(input, upstream)
        (errs ++ refErrors(in, metrics), in) // observation point: data passes through

      // statistics relations: column refs checked, output shapes computed
      // server-side — gradual Unknown downstream
      case Rel.Describe(input, cols) =>
        val (errs, in) = infer(input, upstream)
        (errs ++ nameErrors(in, cols), Shape.Unknown)
      case Rel.Summary(input, _) =>
        (infer(input, upstream)._1, Shape.Unknown)
      case Rel.Crosstab(input, c1, c2) =>
        val (errs, in) = infer(input, upstream)
        (errs ++ nameErrors(in, List(c1, c2)), Shape.Unknown)
      case Rel.Cov(input, c1, c2) =>
        val (errs, in) = infer(input, upstream)
        (errs ++ nameErrors(in, List(c1, c2)), Shape.Unknown)
      case Rel.Corr(input, c1, c2) =>
        val (errs, in) = infer(input, upstream)
        (errs ++ nameErrors(in, List(c1, c2)), Shape.Unknown)
      case Rel.FreqItems(input, cols) =>
        val (errs, in) = infer(input, upstream)
        (errs ++ nameErrors(in, cols), Shape.Unknown)

      case Rel.ToDF(input, columnNames) =>
        val (errs, in) = infer(input, upstream)
        val out = in match
          case Shape.Known(cols) if cols.sizeIs == columnNames.size =>
            Shape.Known(columnNames.zip(cols.map(_._2)))
          case Shape.Known(_) => Shape.Unknown // arity mismatch is the server's diagnostic to give
          case Shape.Unknown  => Shape.Unknown
        (errs, out)

      case Rel.WithColumns(input, columns) =>
        val (errs, in) = infer(input, upstream)
        val refErrs    = refErrors(in, columns.map(_._2))
        val out = in match
          case Shape.Known(cols) =>
            val added = columns.map((n, _) => n -> ColType.Unknown)
            Shape.Known(cols.filterNot((n, _) => columns.exists(_._1 == n)) ++ added)
          case Shape.Unknown => Shape.Unknown
        (errs ++ refErrs, out)

      case Rel.WithColumnsRenamed(input, renames) =>
        val (errs, in) = infer(input, upstream)
        val out = in match
          case Shape.Known(cols) =>
            Shape.Known(cols.map((n, t) => renames.collectFirst { case (`n`, to) => to }.getOrElse(n) -> t))
          case Shape.Unknown => Shape.Unknown
        (errs ++ nameErrors(in, renames.map(_._1)), out)

  // ------------------------------------------------------------------
  // expression column references
  // ------------------------------------------------------------------

  /** Column names an expression references. */
  private def colRefs(ex: Ex): List[String] = ex match
    case Ex.Col(name)      => List(name)
    case Ex.Lit(_)         => Nil
    case Ex.Fn(_, args, _) => args.flatMap(colRefs)
    case Ex.Alias(e, _)    => colRefs(e)
    case Ex.Cast(e, _)     => colRefs(e)
    case Ex.ExprString(_)  => Nil // opaque SQL — server checks
    case Ex.Star(_)        => Nil
    case Ex.ExtractValue(child, _) => colRefs(child) // extraction key isn't a column ref
    case Ex.ColRegex(_)    => Nil
    case Ex.CallFn(_, args) => args.flatMap(colRefs)
    case Ex.Window(f, partitionBy, orderBy, _) =>
      colRefs(f) ++ partitionBy.flatMap(colRefs) ++ orderBy.flatMap(k => colRefs(k.expr))
    case Ex.Lam(_, body)  => colRefs(body) // body Cols are OUTER refs; params are LamVars
    case Ex.LamVar(_)     => Nil           // bound by the enclosing Lam
    case Ex.Subquery(_, kind) =>
      // inner relation has its own column space, and correlation makes
      // checking it against the outer shape unsound — gradual: unchecked.
      kind match
        case SubqueryKind.In(values) => values.flatMap(colRefs)
        case _                       => Nil

  /** The output column an expression contributes (for Project/Aggregate).
    * Bare column pass-throughs keep their input type; computed/aliased
    * expressions are `Unknown` (we don't reimplement Catalyst's typing).
    */
  private def outputColumn(ex: Ex, inCols: List[(String, ColType)]): Option[(String, ColType)] =
    ex match
      case Ex.Alias(Ex.Col(src), name) =>
        Some(name -> inCols.collectFirst { case (`src`, t) => t }.getOrElse(ColType.Unknown))
      case Ex.Alias(Ex.Cast(_, to), name) => Some(name -> to) // cast declares the type
      case Ex.Alias(_, name) => Some(name -> ColType.Unknown)
      case Ex.Col(name) =>
        Some(name -> inCols.collectFirst { case (`name`, t) => t }.getOrElse(ColType.Unknown))
      case _ => None // unaliased computed column: server names it; Unknown shape contribution

  private def refErrors(shape: Shape, exprs: List[Ex]): List[SchemaError] =
    shape.columnNames match
      case None => Nil // gradual: unknown shape, nothing to check
      case Some(available) =>
        exprs.flatMap(colRefs).distinct.collect {
          // qualified refs (a.b) are resolved server-side; check bare names only
          case c if !c.contains('.') && !available.contains(c) =>
            SchemaError.UnknownColumn(c, available)
        }

  private def nameErrors(shape: Shape, names: List[String]): List[SchemaError] =
    refErrors(shape, names.map(Ex.Col(_)))

  private def projectShape(in: Shape, columns: List[Ex]): (List[SchemaError], Shape) =
    val errs = refErrors(in, columns)
    val out = in match
      case Shape.Unknown => Shape.Unknown
      case Shape.Known(inCols) =>
        val outCols = columns.map(outputColumn(_, inCols))
        if outCols.forall(_.isDefined) then Shape.Known(outCols.flatten)
        else Shape.Unknown // a computed, unaliased column — let the server name it
    (errs, out)

  /** Propagate shapes through a whole pipeline in topological order,
    * returning per-flow errors and every dataset's resulting shape. The
    * single entry point shared by validation (errors) and schema codegen
    * (shapes).
    */
  def propagate(
      order: List[String],
      flowsByTarget: Map[String, List[(String, Rel)]], // target -> (flowName, relation)
  ): (List[(String, SchemaError)], Map[String, Shape]) =
    order.foldLeft((List.empty[(String, SchemaError)], Map.empty[String, Shape])) {
      case ((errs, shapes), dataset) =>
        val results = flowsByTarget.getOrElse(dataset, Nil).map { (flowName, relation) =>
          val (flowErrs, shape) = infer(relation, shapes.getOrElse(_, Shape.Unknown))
          (flowErrs.map(flowName -> _), shape)
        }
        val shape = results.map(_._2).collectFirst { case k: Shape.Known => k }.getOrElse(Shape.Unknown)
        (errs ++ results.flatMap(_._1), shapes + (dataset -> shape))
    }
