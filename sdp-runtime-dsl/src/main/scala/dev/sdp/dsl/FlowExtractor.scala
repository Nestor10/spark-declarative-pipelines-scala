package dev.sdp.dsl

import scala.quoted.*

import dev.sdp.core.algebra.*

/** Compile-time interpreter for the fluent flow-body language: a recursive
  * descent over the typed AST producing `Rel`/`Ex` values *in the macro JVM*.
  *
  * Design rules:
  *   - **Environment for `val`s**: block-local bindings of `FlowRel`/`ExArg`
  *     are interpreted and added to scope, so authors can name intermediate
  *     steps — the difference between a DSL and a straitjacket.
  *   - **Bounded language**: anything outside the recognized surface is a
  *     positioned compile error naming what it saw. No silent drops.
  *   - **Errors accumulate** at the reporting layer (`report.error` +
  *     sentinel), consistent with every other macro in this module.
  *
  * Structure note: everything nests inside [[relation]] so a single `Quotes`
  * path governs every `Term` — the path-dependent-types tax of the
  * reflection API.
  */
private[dsl] object FlowExtractor:

  /** Interpret a flow body. On failure, errors were already reported at
    * precise positions; the caller emits the empty-fragment sentinel.
    */
  def relation(using Quotes)(body: quotes.reflect.Term): Option[Rel] =
    import quotes.reflect.*

    /** Marker for `val c = cols[S]` bindings — the handle itself carries no
      * data; selections on it carry the column name in the tree.
      */
    object ColsHandle

    type Bound = Rel | Ex | ColsHandle.type
    type Env   = Map[String, Bound]

    val BinaryOps = Map(
      "+" -> "+", "-" -> "-", "*" -> "*", "/" -> "/", "%" -> "%",
      ">" -> ">", "<" -> "<", ">=" -> ">=", "<=" -> "<=",
      "===" -> "==", "=!=" -> "!=", "&&" -> "and", "||" -> "or",
    )

    object JoinMethod:
      def unapply(name: String): Option[JoinType] = name match
        case "join"      => Some(JoinType.Inner)
        case "joinLeft"  => Some(JoinType.LeftOuter)
        case "joinRight" => Some(JoinType.RightOuter)
        case "joinFull"  => Some(JoinType.FullOuter)
        case "joinSemi"  => Some(JoinType.LeftSemi)
        case "joinAnti"  => Some(JoinType.LeftAnti)
        case _           => None

    val SchemaTypes = Map(
      "bool"      -> ColType.Bool,
      "int"       -> ColType.I32,
      "long"      -> ColType.I64,
      "double"    -> ColType.F64,
      "string"    -> ColType.Str,
      "timestamp" -> ColType.Timestamp,
      "date"      -> ColType.Date,
    )

    /** The curated `functions` facade — Scala name → wire name. Mismatches
      * follow Spark's own `functions.scala` (authority rule): `pow` → `power`,
      * `mean` → `avg`. The stubs in `Functions.scala` own typing and arity;
      * here every entry lowers uniformly to `Ex.Fn(wireName, args)`.
      */
    val FacadeFunctions: Map[String, String] =
      val identity = List(
        "count", "sum", "avg", "min", "max", "first", "last", "collect_list", "collect_set",
        "row_number", "rank", "dense_rank", "ntile", "lag", "lead",
        "upper", "lower", "trim", "length", "concat", "concat_ws", "substring", "split",
        "regexp_replace",
        "to_date", "to_timestamp", "date_trunc", "year", "month", "dayofmonth", "hour",
        "minute", "second", "current_date", "current_timestamp", "date_add", "date_sub",
        "datediff",
        "abs", "round", "floor", "ceil", "sqrt", "exp", "log", "greatest", "least",
        "coalesce", "isnull", "isnan", "when",
        "struct", "array", "map", "explode", "explode_outer", "size", "element_at",
        "array_contains", "map_keys", "map_values",
        "transform", "filter", "exists", "forall", "aggregate", "zip_with",
        "hash", "md5", "sha2", "monotonically_increasing_id", "rand",
      )
      identity.map(n => n -> n).toMap + ("mean" -> "avg") + ("pow" -> "power")

    object SetOpMethod:
      def unapply(name: String): Option[(SetOpType, Boolean)] = name match
        case "union"        => Some((SetOpType.Union, true)) // Spark union = UNION ALL
        case "intersect"    => Some((SetOpType.Intersect, false))
        case "except"       => Some((SetOpType.Except, false))
        case "intersectAll" => Some((SetOpType.Intersect, true))
        case "exceptAll"    => Some((SetOpType.Except, true))
        case _              => None

    def str(term: Term, what: String): Option[String] = term match
      case Literal(StringConstant(s)) => Some(s)
      case Inlined(_, Nil, inner)     => str(inner, what)
      case other =>
        report.error(s"$what must be a constant string literal; got: ${other.show}", other.pos)
        None

    /** Varargs arrive as `Typed(Repeated(items, _), _)`. */
    def flattenVarargs(args: List[Term]): List[Term] = args.flatMap {
      case Typed(Repeated(items, _), _) => items
      case Repeated(items, _)           => items
      case single                       => List(single)
    }

    def traverse[A](items: List[Term])(f: Term => Option[A]): Option[List[A]] =
      items.foldRight(Option(List.empty[A])) { (item, acc) =>
        for { a <- f(item); rest <- acc } yield a :: rest
      }

    // ----------------------------------------------------------------
    // expressions
    // ----------------------------------------------------------------

    def ex(term: Term, env: Env): Option[Ex] = term match
      case Inlined(_, Nil, inner) => ex(inner, env)
      case Typed(inner, _)        => ex(inner, env)
      // structural selections come wrapped in a cast to the field type
      case TypeApply(Select(inner, "$asInstanceOf$"), _) => ex(inner, env)

      // typed column reference: c.amount ~> c.selectDynamic("amount").
      // The receiver is a phantom — the column name in the tree is the data.
      case Apply(Select(_, "selectDynamic"), List(arg)) =>
        str(arg, "column selection").map(Ex.Col(_))

      case Ident(name) if env.contains(name) =>
        env(name) match
          case e: Ex => Some(e)
          case ColsHandle =>
            report.error(
              s"'$name' is a column handle — select a column from it (e.g. $name.some_column)",
              term.pos,
            )
            None
          case _: Rel =>
            report.error(s"'$name' is a relation, but an expression is required here", term.pos)
            None

      case Apply(Ident("col"), List(arg)) =>
        str(arg, "col name").map(Ex.Col(_))

      case Apply(Ident("lit"), List(arg)) =>
        arg match
          case Literal(IntConstant(v))     => Some(Ex.Lit(LitValue.I32(v)))
          case Literal(LongConstant(v))    => Some(Ex.Lit(LitValue.I64(v)))
          case Literal(DoubleConstant(v))  => Some(Ex.Lit(LitValue.F64(v)))
          case Literal(BooleanConstant(v)) => Some(Ex.Lit(LitValue.Bool(v)))
          case Literal(StringConstant(v))  => Some(Ex.Lit(LitValue.Str(v)))
          case other =>
            report.error(s"lit requires a constant literal; got: ${other.show}", other.pos)
            None

      case Apply(Ident("fn"), name :: args) =>
        for
          n  <- str(name, "fn name")
          as <- traverse(flattenVarargs(args))(ex(_, env))
        yield Ex.Fn(n, as)

      case Apply(Select(receiver, "as"), List(arg)) =>
        for
          e <- ex(receiver, env)
          n <- str(arg, "alias name")
        yield Ex.Alias(e, n)

      case Apply(Ident("expr"), List(arg)) =>
        str(arg, "expr SQL").map(Ex.ExprString(_))

      // real Scala lambdas: the author's parameter names become the wire's
      // lambda variables; the body interprets with params bound to LamVars
      case Apply(Ident(m @ ("lam" | "lam2")), List(fnTerm)) =>
        lambdaTerm(fnTerm) match
          case Some((params, body)) =>
            val expected = if m == "lam" then 1 else 2
            if params.sizeIs != expected then
              report.error(s"$m expects a $expected-parameter lambda", term.pos)
              None
            else
              val bound = params.map(p => p -> (Ex.LamVar(p): Bound)).toMap
              ex(body, env ++ bound).map(Ex.Lam(params, _))
          case None =>
            report.error(s"$m expects a lambda literal, e.g. lam(x => x + lit(1))", term.pos)
            None

      // single-arg exists = the subquery form (overload resolution guarantees
      // the argument typed as FlowRel); the array-HOF exists has two args and
      // falls through to the facade case below
      case Apply(Ident(m @ ("exists" | "scalar")), List(relTerm)) =>
        rel(relTerm, env).map { r =>
          Ex.Subquery(r, if m == "exists" then SubqueryKind.Exists else SubqueryKind.Scalar)
        }

      // the curated functions facade: any stubbed Spark name lowers to
      // Ex.Fn(wireName); HOF lambdas extract directly — no lam() wrapper
      case Apply(Ident(name), args) if FacadeFunctions.contains(name) =>
        traverse(flattenVarargs(args))(fnArg(_, env)).map(Ex.Fn(FacadeFunctions(name), _))
      case Apply(Select(Ident("functions"), name), args) if FacadeFunctions.contains(name) =>
        traverse(flattenVarargs(args))(fnArg(_, env)).map(Ex.Fn(FacadeFunctions(name), _))

      // CASE chaining: .when(c, v) appends a branch, .otherwise(e) the else —
      // one UnresolvedFunction("when", [c1, v1, ..., else]) on the wire,
      // verified against Spark's ColumnNodeToProtoConverter
      case Apply(Select(receiver, "when"), List(c, v)) =>
        ex(receiver, env).flatMap {
          case Ex.Fn("when", branches, _) =>
            for cc <- ex(c, env); vv <- ex(v, env)
            yield Ex.Fn("when", branches ++ List(cc, vv))
          case _ =>
            report.error(".when(...) chains only onto when(cond, value)", term.pos)
            None
        }
      case Apply(Select(receiver, "otherwise"), List(elseArg)) =>
        ex(receiver, env).flatMap {
          case Ex.Fn("when", branches, _) =>
            ex(elseArg, env).map(v => Ex.Fn("when", branches :+ v))
          case _ =>
            report.error(".otherwise(...) is only valid after when(cond, value)", term.pos)
            None
        }

      case Apply(Select(lhs, "in"), List(relTerm)) =>
        for
          l <- ex(lhs, env)
          r <- rel(relTerm, env)
        yield Ex.Subquery(r, SubqueryKind.In(List(l)))

      // Column.isin(lits*) — membership in a literal set; lowers to the SQL
      // `in` function so it needs no subquery plan
      case Apply(Select(lhs, "isin"), args) =>
        for
          l  <- ex(lhs, env)
          vs <- traverse(flattenVarargs(args))(ex(_, env))
        yield Ex.Fn("in", l :: vs)

      case Ident("star") => Some(Ex.Star(None))

      case Apply(Select(receiver, "getItem"), List(key)) =>
        for
          c <- ex(receiver, env)
          k <- ex(key, env)
        yield Ex.ExtractValue(c, k)

      case Apply(Select(receiver, "getField"), List(nameArg)) =>
        for
          c <- ex(receiver, env)
          n <- str(nameArg, "getField name")
        yield Ex.ExtractValue(c, Ex.Lit(LitValue.Str(n)))

      case Apply(Select(receiver, "over"), List(specArg)) =>
        for
          f    <- ex(receiver, env)
          spec <- windowSpec(specArg, env)
        yield Ex.Window(f, spec._1, spec._2, None)

      case Apply(Select(receiver, "cast"), List(typeArg)) =>
        for
          e <- ex(receiver, env)
          t <- typeArg match
            case Ident(tok) if SchemaTypes.contains(tok) => Some(SchemaTypes(tok))
            case other =>
              report.error(
                s"cast target must be one of: ${SchemaTypes.keys.toList.sorted.mkString(", ")}",
                other.pos,
              )
              None
        yield Ex.Cast(e, t)

      case Apply(Select(left, op), List(right)) if BinaryOps.contains(op) =>
        for
          l <- ex(left, env)
          r <- ex(right, env)
        yield Ex.Fn(BinaryOps(op), List(l, r))

      case other =>
        report.error(
          s"Unsupported expression: ${other.show}. Supported: col, lit, fn, operators " +
            s"(${BinaryOps.keys.toList.sorted.mkString(" ")}), .as, .asc, .desc.",
          other.pos,
        )
        None

    /** A column argument that may be a bare string (Spark's
      * `groupBy("a", "b")` / `select("a")` overloads) or any expression. */
    def colArg(term: Term, env: Env): Option[Ex] = term match
      case Literal(StringConstant(s)) => Some(Ex.Col(s))
      case Inlined(_, Nil, inner)     => colArg(inner, env)
      case other                      => ex(other, env)

    def sortKey(term: Term, env: Env): Option[SortKey] = term match
      case Literal(StringConstant(s)) => Some(SortKey(Ex.Col(s))) // orderBy("col") = ascending
      case Select(inner, "asc")  => ex(inner, env).map(SortKey(_, descending = false))
      case Select(inner, "desc") => ex(inner, env).map(SortKey(_, descending = true))
      case other                 => ex(other, env).map(SortKey(_))

    /** Dig the key string out of an `"a" -> v` arrow (the `ArrowAssoc` wrapper)
      * or a `Tuple2("a", v)`. */
    /** Match an `a -> b` arrow, seeing through the `[V]` type application the
      * `ArrowAssoc.->` carries, or a `Tuple2(a, b)`. */
    object Arrow:
      def unapply(term: Term): Option[(Term, Term)] = term match
        case Apply(TypeApply(Select(keyWrap, "->"), _), List(value)) => Some((keyWrap, value))
        case Apply(Select(keyWrap, "->"), List(value))              => Some((keyWrap, value))
        case Apply(TypeApply(Select(Ident("Tuple2"), "apply"), _), List(k, v)) => Some((k, v))
        case _ => None

    def mapEntry(term: Term, env: Env): Option[(String, Ex)] = term match
      case Inlined(_, Nil, inner) => mapEntry(inner, env)
      case Typed(inner, _)        => mapEntry(inner, env)
      case Arrow(keyWrap, valueTerm) =>
        for { k <- arrowKey(keyWrap); v <- ex(valueTerm, env) } yield k -> v
      case other =>
        report.error(s"expected a `name -> value` entry; got: ${other.show}", other.pos)
        None

    /** Same as [[mapEntry]] but both sides are strings (renames). */
    def renameEntry(term: Term): Option[(String, String)] = term match
      case Inlined(_, Nil, inner) => renameEntry(inner)
      case Typed(inner, _)        => renameEntry(inner)
      case Arrow(keyWrap, valueTerm) =>
        for { k <- arrowKey(keyWrap); v <- str(valueTerm, "rename target") } yield k -> v
      case other =>
        report.error(s"expected a `from -> to` entry; got: ${other.show}", other.pos)
        None

    /** The string key inside an `ArrowAssoc("a")` application. */
    def arrowKey(term: Term): Option[String] = term match
      case Inlined(_, _, inner)           => arrowKey(inner)
      case Typed(inner, _)                => arrowKey(inner)
      case Apply(_, List(arg))            => str(arg, "map key")
      case TypeApply(inner, _)            => arrowKey(inner)
      case Literal(StringConstant(s))     => Some(s)
      case other =>
        report.error(s"map key must be a string literal; got: ${other.show}", other.pos)
        None

    /** The entry terms of a `Map(...)` literal, in source order. */
    def mapLiteralEntries(term: Term): Option[List[Term]] = term match
      case Inlined(_, Nil, inner) => mapLiteralEntries(inner)
      case Typed(inner, _)        => mapLiteralEntries(inner)
      case Apply(TypeApply(Select(Ident("Map"), "apply"), _), args) => Some(flattenVarargs(args))
      case Apply(Select(Ident("Map"), "apply"), args)               => Some(flattenVarargs(args))
      case other =>
        report.error(s"expected a Map(...) literal; got: ${other.show}", other.pos)
        None

    /** The string items of a `Seq(...)` / `List(...)` literal. */
    def seqLiteralStrings(term: Term, what: String): Option[List[String]] = term match
      case Inlined(_, Nil, inner) => seqLiteralStrings(inner, what)
      case Typed(inner, _)        => seqLiteralStrings(inner, what)
      case Apply(TypeApply(Select(Ident("Seq" | "List"), "apply"), _), args) =>
        traverse(flattenVarargs(args))(str(_, what))
      case Apply(Select(Ident("Seq" | "List"), "apply"), args) =>
        traverse(flattenVarargs(args))(str(_, what))
      case other =>
        report.error(s"expected a Seq(...) of string literals for $what; got: ${other.show}", other.pos)
        None

    /** A `oldLit -> newLit` entry for na.replace. */
    def replaceEntry(term: Term, env: Env): Option[(LitValue, LitValue)] = term match
      case Inlined(_, Nil, inner) => replaceEntry(inner, env)
      case Typed(inner, _)        => replaceEntry(inner, env)
      case Arrow(keyWrap, valueTerm) =>
        for { k <- arrowLit(keyWrap, env); v <- litOf(valueTerm, env, "na.replace value") } yield k -> v
      case other =>
        report.error(s"expected a `oldValue -> newValue` entry; got: ${other.show}", other.pos)
        None

    /** The literal inside an `ArrowAssoc(lit(...))` application. */
    def arrowLit(term: Term, env: Env): Option[LitValue] = term match
      case Inlined(_, _, inner) => arrowLit(inner, env)
      case Typed(inner, _)      => arrowLit(inner, env)
      case TypeApply(inner, _)  => arrowLit(inner, env)
      case Apply(_, List(arg))  => litOf(arg, env, "na.replace key")
      case other                => litOf(other, env, "na.replace key")

    /** An expression that must be a literal (na.fill / na.replace values). */
    def litOf(term: Term, env: Env, what: String): Option[LitValue] =
      ex(term, env).flatMap {
        case Ex.Lit(v) => Some(v)
        case _ =>
          report.error(s"$what must be a lit(...)", term.pos)
          None
      }

    /** `df.na.{drop,fill,replace}` → DropNa / FillNa / Replace. */
    def naFunction(receiver: Term, method: String, args: List[Term], env: Env): Option[Rel] =
      rel(receiver, env).flatMap { input =>
        (method, args) match
          case ("drop", Nil)            => Some(Rel.DropNa(input, Nil))
          case ("drop", List(seqArg))   => seqLiteralStrings(seqArg, "na.drop column").map(Rel.DropNa(input, _))
          case ("fill", List(valueArg)) => litOf(valueArg, env, "na.fill value").map(Rel.FillNa(input, Nil, _))
          case ("fill", List(valueArg, seqArg)) =>
            for
              v  <- litOf(valueArg, env, "na.fill value")
              cs <- seqLiteralStrings(seqArg, "na.fill column")
            yield Rel.FillNa(input, cs, v)
          case ("replace", List(colArg, mapArg)) =>
            for
              cols    <- colArg match
                case Literal(StringConstant(c)) => Some(List(c))
                case seq                        => seqLiteralStrings(seq, "na.replace column")
              entries <- mapLiteralEntries(mapArg).flatMap(traverse(_)(replaceEntry(_, env)))
            yield Rel.Replace(input, cols, entries)
          case _ =>
            report.error(s"unsupported na.$method(...) shape", receiver.pos)
            None
      }

    /** `df.stat.{crosstab,cov,corr,freqItems}`. */
    def statFunction(receiver: Term, method: String, args: List[Term], env: Env, whole: Term): Option[Rel] =
      rel(receiver, env).flatMap { input =>
        (method, args) match
          case ("crosstab", List(a, b)) =>
            for { c1 <- str(a, "crosstab column"); c2 <- str(b, "crosstab column") }
            yield Rel.Crosstab(input, c1, c2)
          case ("cov", List(a, b)) =>
            for { c1 <- str(a, "cov column"); c2 <- str(b, "cov column") } yield Rel.Cov(input, c1, c2)
          case ("corr", List(a, b)) =>
            for { c1 <- str(a, "corr column"); c2 <- str(b, "corr column") } yield Rel.Corr(input, c1, c2)
          case ("freqItems", List(seqArg)) =>
            seqLiteralStrings(seqArg, "freqItems column").map(Rel.FreqItems(input, _))
          case _ =>
            report.error(s"unsupported stat.$method(...) shape", whole.pos)
            None
      }

    /** A facade-function argument: a real Scala lambda (HOFs), a bare literal
      * (Int offsets, String formats — Spark's signatures take plain params),
      * or any expression.
      */
    def fnArg(term: Term, env: Env): Option[Ex] =
      lambdaTerm(term) match
        case Some((params, body)) =>
          val bound = params.map(p => p -> (Ex.LamVar(p): Bound)).toMap
          ex(body, env ++ bound).map(Ex.Lam(params, _))
        case None =>
          term match
            case Literal(IntConstant(v))     => Some(Ex.Lit(LitValue.I32(v)))
            case Literal(LongConstant(v))    => Some(Ex.Lit(LitValue.I64(v)))
            case Literal(DoubleConstant(v))  => Some(Ex.Lit(LitValue.F64(v)))
            case Literal(BooleanConstant(v)) => Some(Ex.Lit(LitValue.Bool(v)))
            case Literal(StringConstant(v))  => Some(Ex.Lit(LitValue.Str(v)))
            case _                           => ex(term, env)

    /** A lambda literal: param names + body term. */
    def lambdaTerm(term: Term): Option[(List[String], Term)] = term match
      case Inlined(_, Nil, inner) => lambdaTerm(inner)
      case Typed(inner, _)        => lambdaTerm(inner)
      case Block(List(dd: DefDef), Closure(_, _)) =>
        val params = dd.termParamss.flatMap(_.params.map(_.name))
        dd.rhs.map(params -> _)
      case _ => None

    /** Interpret `window.partitionBy(...).orderBy(...)` (either part optional)
      * into (partition columns, order keys).
      */
    def windowSpec(term: Term, env: Env): Option[(List[Ex], List[SortKey])] = term match
      case Inlined(_, Nil, inner) => windowSpec(inner, env)
      case Typed(inner, _)        => windowSpec(inner, env)
      case Apply(Select(Ident("window" | "Window"), "partitionBy"), args) =>
        traverse(flattenVarargs(args))(ex(_, env)).map((_, Nil))
      case Apply(Select(Ident("window" | "Window"), "orderBy"), args) =>
        traverse(flattenVarargs(args))(sortKey(_, env)).map((Nil, _))
      case Apply(Select(receiver, "orderBy"), args) =>
        for
          (parts, _) <- windowSpec(receiver, env)
          keys       <- traverse(flattenVarargs(args))(sortKey(_, env))
        yield (parts, keys)
      case other =>
        report.error(
          s"Unsupported window spec: ${other.show}. Use window.partitionBy(cols*) and/or .orderBy(keys*).",
          other.pos,
        )
        None

    // ----------------------------------------------------------------
    // relations
    // ----------------------------------------------------------------

    /** `range(...)` leaf shared by `spark.range` (1–3 args, Spark-style
      * single-arg = `0 until end`) and `read.range` (2–3 args).
      */
    def rangeRel(args: List[Term], term: Term, what: String, allowSingle: Boolean): Option[Rel] =
      traverse(args) {
        case Literal(LongConstant(v)) => Some(v)
        case Literal(IntConstant(v))  => Some(v.toLong)
        case other =>
          report.error(s"$what arguments must be integer literals; got: ${other.show}", other.pos)
          None
      }.flatMap {
        case List(end) if allowSingle => Some(Rel.Range(0L, end, 1L))
        case List(start, end)         => Some(Rel.Range(start, end, 1L))
        case List(start, end, step)   => Some(Rel.Range(start, end, step))
        case _ =>
          val shapes = if allowSingle then "(end), (start, end) or (start, end, step)"
                       else "(start, end) or (start, end, step)"
          report.error(s"$what takes $shapes", term.pos)
          None
      }

    // ----------------------------------------------------------------
    // inline data: spark.createDataFrame(Seq(...)).toDF(...) → Rel.LocalData
    // ----------------------------------------------------------------

    /** One literal cell (`null` allowed — its column type comes from siblings). */
    def litCell(t: Term): Option[LitValue] = t match
      case Literal(IntConstant(v))     => Some(LitValue.I32(v))
      case Literal(LongConstant(v))    => Some(LitValue.I64(v))
      case Literal(DoubleConstant(v))  => Some(LitValue.F64(v))
      case Literal(BooleanConstant(v)) => Some(LitValue.Bool(v))
      case Literal(StringConstant(v))  => Some(LitValue.Str(v))
      case Literal(NullConstant())     => Some(LitValue.Null)
      case Inlined(_, Nil, inner)      => litCell(inner)
      case Typed(inner, _)             => litCell(inner)
      case _                           => None

    def litCellChecked(t: Term): Option[LitValue] = litCell(t).orElse {
      report.error(
        s"inline-table cells must be literal Int/Long/Double/Boolean/String or null; got: ${t.show}",
        t.pos,
      )
      None
    }

    /** Tuple constructor elements, if `t` is `(a, b, ...)`. */
    def tupleElems(t: Term): Option[List[Term]] = t match
      case Apply(TypeApply(Select(ref, "apply"), _), elems) if ref.symbol.fullName.startsWith("scala.Tuple") =>
        Some(elems)
      case Apply(Select(ref, "apply"), elems) if ref.symbol.fullName.startsWith("scala.Tuple") =>
        Some(elems)
      case _ => None

    /** One row: a tuple (multi-column) or a bare literal (single-column). */
    def inlineRow(t: Term): Option[List[LitValue]] = t match
      case Inlined(_, Nil, inner) => inlineRow(inner)
      case Typed(inner, _)        => inlineRow(inner)
      case _ =>
        tupleElems(t) match
          case Some(elems) => traverse(elems)(litCellChecked)
          case None        => litCellChecked(t).map(List(_))

    /** Rows from the `Seq(...)`/`List(...)` argument of createDataFrame. */
    def inlineRows(t: Term): Option[List[List[LitValue]]] = t match
      case Inlined(_, Nil, inner)                        => inlineRows(inner)
      case Typed(inner, _)                               => inlineRows(inner)
      case Apply(TypeApply(Select(_, "apply"), _), args) => traverse(flattenVarargs(args))(inlineRow)
      case Apply(Select(_, "apply"), args)               => traverse(flattenVarargs(args))(inlineRow)
      case other =>
        report.error(s"spark.createDataFrame expects an inline Seq of literal rows; got: ${other.show}", other.pos)
        None

    def numericRank = Map(ColType.I32 -> 1, ColType.I64 -> 2, ColType.F64 -> 3)
    def litType(v: LitValue): ColType = v match
      case LitValue.Bool(_) => ColType.Bool
      case LitValue.I32(_)  => ColType.I32
      case LitValue.I64(_)  => ColType.I64
      case LitValue.F64(_)  => ColType.F64
      case LitValue.Str(_)  => ColType.Str
      case LitValue.Null    => ColType.Unknown

    /** Per-column type: numeric kinds widen (I32 < I64 < F64); null cells take
      * the column type; mixed non-numeric kinds are an error. */
    def inferColType(cells: List[LitValue], pos: Term): Option[ColType] =
      val kinds = cells.filterNot(_ == LitValue.Null).map(litType).distinct
      if kinds.isEmpty then
        report.error("cannot infer the type of an all-null inline-table column; add one typed value", pos.pos)
        None
      else if kinds.forall(numericRank.contains) then Some(kinds.maxBy(numericRank))
      else if kinds.sizeIs == 1 then Some(kinds.head)
      else
        report.error(s"inconsistent inline-table column types: ${kinds.mkString(", ")}", pos.pos)
        None

    /** Assemble Rel.LocalData: arity check, names (toDF or `_1.._N`), types. */
    def localData(rows: List[List[LitValue]], names: Option[List[String]], pos: Term): Option[Rel] =
      if rows.isEmpty then
        report.error("spark.createDataFrame needs at least one row", pos.pos)
        None
      else
        val width = rows.head.size
        if rows.exists(_.size != width) then
          report.error("every row in spark.createDataFrame must have the same number of columns", pos.pos)
          None
        else
          val cols = names.getOrElse((1 to width).map(i => s"_$i").toList)
          if cols.size != width then
            report.error(s".toDF gave ${cols.size} name(s) for a $width-column inline table", pos.pos)
            None
          else
            val types = (0 until width).toList.foldRight(Option(List.empty[ColType])) { (j, acc) =>
              for { t <- inferColType(rows.map(_(j)), pos); rest <- acc } yield t :: rest
            }
            types.map(ts => Rel.LocalData(cols.zip(ts), rows))

    def rel(term: Term, env: Env): Option[Rel] = term match
      // blocks: interpret val-bindings into the environment
      case Block(stats, expr) =>
        stats
          .foldLeft(Option(env)) {
            case (Some(e), ValDef(name, _, Some(rhs))) => bind(rhs, e).map(b => e + (name -> b))
            case (Some(_), other) =>
              report.error(
                s"Only `val` definitions are allowed inside flow bodies; got: ${other.show}",
                other.pos,
              )
              None
            case (None, _) => None
          }
          .flatMap(e => rel(expr, e))

      case Inlined(_, Nil, inner) => rel(inner, env)
      case Typed(inner, _)        => rel(inner, env)

      case Ident(name) if env.contains(name) =>
        env(name) match
          case r: Rel => Some(r)
          case _: Ex =>
            report.error(s"'$name' is an expression, but a relation is required here", term.pos)
            None

      // leaves — the SparkSession facade (the documented surface)
      case Apply(Select(Ident("spark"), "table"), List(arg)) =>
        str(arg, "spark.table name").map(Rel.NamedTable(_, streaming = false))
      case Apply(Select(Ident("spark"), "sql"), List(arg)) =>
        str(arg, "spark.sql query").map(Rel.Sql(_))
      case Apply(Select(Ident("spark"), "range"), args) =>
        rangeRel(args, term, "spark.range", allowSingle = true)
      case Apply(Select(Select(Ident("spark"), "read"), "table"), List(arg)) =>
        str(arg, "spark.read.table name").map(Rel.NamedTable(_, streaming = false))
      case Apply(Select(Select(Ident("spark"), "readStream"), "table"), List(arg)) =>
        str(arg, "spark.readStream.table name").map(Rel.NamedTable(_, streaming = true))
      case Apply(Select(Select(Ident("spark"), "read"), "format"), List(arg)) =>
        str(arg, "spark.read.format source").map(Rel.DataSource(_, Map.empty, streaming = false))
      case Apply(Select(Select(Ident("spark"), "readStream"), "format"), List(arg)) =>
        str(arg, "spark.readStream.format source").map(Rel.DataSource(_, Map.empty, streaming = true))

      // inline literal table — `createDataFrame[A]` is generic, so the call
      // carries a `TypeApply`. Fold `.toDF(names)` into the schema directly so
      // the columns are named at the source (vs an extra ToDF wrapper).
      case Apply(
            Select(Apply(TypeApply(Select(Ident("spark"), "createDataFrame"), _), List(data)), "toDF"),
            nameArgs,
          ) =>
        for
          rows  <- inlineRows(data)
          names <- traverse(flattenVarargs(nameArgs))(str(_, "toDF column name"))
          ld    <- localData(rows, Some(names), term)
        yield ld
      case Apply(TypeApply(Select(Ident("spark"), "createDataFrame"), _), List(data)) =>
        inlineRows(data).flatMap(rows => localData(rows, None, term))

      // load() terminates a reader chain Spark-style; the path variant records
      // the path as the `path` option, exactly as Spark does
      case Apply(Select(receiver, "load"), args) =>
        for
          input <- rel(receiver, env)
          path <- args match
            case Nil       => Some(None)
            case List(arg) => str(arg, "load path").map(Some(_))
            case _ =>
              report.error("load takes at most one (path) argument", term.pos)
              None
          out <- input match
            case ds: Rel.DataSource =>
              Some(path.fold(ds)(p => ds.copy(options = ds.options + ("path" -> p))))
            case _ =>
              report.error(".load(...) is only valid on a format(...) reader chain", term.pos)
              None
        yield out

      // leaves — pre-facade spellings
      case Apply(Select(Ident("stream"), "table"), List(arg)) =>
        str(arg, "stream.table name").map(Rel.NamedTable(_, streaming = true))
      case Apply(Select(Ident("read"), "table"), List(arg)) =>
        str(arg, "read.table name").map(Rel.NamedTable(_, streaming = false))
      case Apply(Select(Ident("stream"), "source"), List(arg)) =>
        str(arg, "stream.source format").map(Rel.DataSource(_, Map.empty, streaming = true))
      case Apply(Select(Ident("read"), "range"), args) =>
        rangeRel(args, term, "read.range", allowSingle = false)

      // source options chain onto the DataSource leaf
      case Apply(Select(receiver, "option"), List(k, v)) =>
        for
          input <- rel(receiver, env)
          key   <- str(k, "option key")
          value <- str(v, "option value")
          out <- input match
            case ds: Rel.DataSource => Some(ds.copy(options = ds.options + (key -> value)))
            case _ =>
              report.error(".option(...) is only valid directly on stream.source(...)", term.pos)
              None
        yield out

      // Spark's DDL schema string, parsed at compile time (DdlSchema is the
      // pure parser in sdp-core); the column list lands on the DataSource
      // leaf exactly as withSchema's does
      case Apply(Select(receiver, "schema"), List(ddlArg)) =>
        for
          input <- rel(receiver, env)
          ddl   <- str(ddlArg, "schema DDL")
          fields <- DdlSchema.parse(ddl) match
            case Right(fs) => Some(fs)
            case Left(msg) =>
              report.error(s"invalid schema DDL: $msg", ddlArg.pos)
              None
          out <- input match
            // keep the RAW ddl verbatim for the wire; the parsed `fields` drive
            // local inference (lossy for DECIMAL/ARRAY/… — but the wire is exact).
            case ds: Rel.DataSource => Some(ds.copy(schema = fields, schemaDdl = Some(ddl)))
            case _ =>
              report.error(".schema(...) is only valid on a format(...) reader chain", term.pos)
              None
        yield out

      // declared source schema chains onto the DataSource leaf
      case Apply(Select(receiver, "withSchema"), args) =>
        for
          input <- rel(receiver, env)
          fields <- traverse(flattenVarargs(args)) {
            case Apply(Ident("field"), List(nameArg, typeArg)) =>
              for
                name <- str(nameArg, "field name")
                tpe <- typeArg match
                  case Ident(t) if SchemaTypes.contains(t) => Some(SchemaTypes(t))
                  case other =>
                    report.error(
                      s"field type must be one of: ${SchemaTypes.keys.toList.sorted.mkString(", ")}",
                      other.pos,
                    )
                    None
              yield name -> tpe
            case other =>
              report.error(s"withSchema takes field(name, type) entries; got: ${other.show}", other.pos)
              None
          }
          out <- input match
            case ds: Rel.DataSource =>
              // render a DDL from the (all-checked) field tokens for the wire
              Some(ds.copy(schema = fields, schemaDdl = Some(DdlSchema.render(fields))))
            case _ =>
              report.error(".withSchema(...) is only valid directly on stream.source(...)", term.pos)
              None
        yield out

      // df.na.* / df.stat.* — namespace facades; matched BEFORE the flat
      // crosstab/cov/corr/dropNa cases so `x.stat.corr` doesn't get read as
      // the flat `corr` with receiver `x.stat`
      case Apply(Select(Select(receiver, "na"), method @ ("drop" | "fill" | "replace")), args) =>
        naFunction(receiver, method, args, env)
      case Apply(Select(Select(receiver, "stat"), method @ ("crosstab" | "cov" | "corr" | "freqItems")), args) =>
        statFunction(receiver, method, args, env, term)

      // transformations
      case Apply(Select(receiver, "select"), args) =>
        for
          input   <- rel(receiver, env)
          columns <- traverse(flattenVarargs(args))(colArg(_, env))
        yield Rel.Project(input, columns)

      case Apply(Select(receiver, "where" | "filter"), List(cond)) =>
        for
          input <- rel(receiver, env)
          c     <- ex(cond, env)
        yield Rel.Filter(input, c)

      // groupBy(...).agg(...) — matched outside-in
      case Apply(Select(Apply(Select(receiver, "groupBy"), groupArgs), "agg"), aggArgs) =>
        for
          input  <- rel(receiver, env)
          groups <- traverse(flattenVarargs(groupArgs))(colArg(_, env))
          aggs   <- traverse(flattenVarargs(aggArgs))(ex(_, env))
        yield Rel.Aggregate(input, groups, aggs)

      // groupBy(...).count() — Spark convenience: a `count` column
      case Apply(Select(Apply(Select(receiver, "groupBy"), groupArgs), "count"), Nil) =>
        for
          input  <- rel(receiver, env)
          groups <- traverse(flattenVarargs(groupArgs))(colArg(_, env))
        yield Rel.Aggregate(input, groups, List(Ex.Alias(Ex.Fn("count", List(Ex.Star(None))), "count")))

      case Apply(Apply(Select(receiver, JoinMethod(joinType)), List(right)), List(cond)) =>
        for
          l <- rel(receiver, env)
          r <- rel(right, env)
          c <- ex(cond, env)
        yield Rel.Join(l, r, Some(c), joinType)

      case Apply(Select(receiver, "crossJoin"), List(right)) =>
        for
          l <- rel(receiver, env)
          r <- rel(right, env)
        yield Rel.Join(l, r, None, JoinType.Cross)

      case Apply(Select(receiver, SetOpMethod(op, all)), List(right)) =>
        for
          l <- rel(receiver, env)
          r <- rel(right, env)
        yield Rel.SetOp(l, r, op, all)

      case Select(receiver, "distinct") =>
        rel(receiver, env).map(Rel.Deduplicate(_, Nil))

      case Apply(Select(receiver, "dropDuplicates"), args) =>
        for
          input <- rel(receiver, env)
          cols  <- traverse(flattenVarargs(args))(str(_, "dropDuplicates column"))
        yield Rel.Deduplicate(input, cols)

      case Apply(Select(receiver, "drop"), args) =>
        for
          input <- rel(receiver, env)
          cols  <- traverse(flattenVarargs(args))(str(_, "drop column"))
        yield Rel.Drop(input, cols)

      case Apply(Select(receiver, "alias"), List(arg)) =>
        for
          input <- rel(receiver, env)
          name  <- str(arg, "alias name")
        yield Rel.SubqueryAlias(input, name)

      case Apply(Select(receiver, "toDF"), args) =>
        for
          input <- rel(receiver, env)
          names <- traverse(flattenVarargs(args))(str(_, "toDF column name"))
        yield Rel.ToDF(input, names)

      case Apply(Select(receiver, "withColumn"), List(nameArg, valueArg)) =>
        for
          input <- rel(receiver, env)
          name  <- str(nameArg, "withColumn name")
          value <- ex(valueArg, env)
        yield input match
          // consecutive withColumn calls collapse into one WithColumns
          case Rel.WithColumns(inner, cols) => Rel.WithColumns(inner, cols :+ (name -> value))
          case other                        => Rel.WithColumns(other, List(name -> value))

      // withColumns(Map("a" -> e, ...)) — Spark's Map signature; entries are
      // read in source order so the manifest stays deterministic
      case Apply(Select(receiver, "withColumns"), List(mapArg)) =>
        for
          input   <- rel(receiver, env)
          entries <- mapLiteralEntries(mapArg).flatMap(traverse(_)(mapEntry(_, env)))
        yield Rel.WithColumns(input, entries)

      case Apply(Select(receiver, "sample"), args) =>
        for
          input <- rel(receiver, env)
          out <- args match
            case List(Literal(DoubleConstant(f))) => Some(Rel.Sample(input, f, None))
            case List(Literal(DoubleConstant(f)), Literal(LongConstant(s))) =>
              Some(Rel.Sample(input, f, Some(s)))
            case _ =>
              report.error("sample takes literal (fraction[, seed]) arguments", term.pos)
              None
        yield out

      case Apply(Select(receiver, "hint"), nameArg :: params) =>
        for
          input <- rel(receiver, env)
          name  <- str(nameArg, "hint name")
          ps    <- traverse(flattenVarargs(params))(ex(_, env))
        yield Rel.Hint(input, name, ps)

      case Apply(Select(receiver, method @ ("repartition" | "coalesce")), List(n)) =>
        for
          input <- rel(receiver, env)
          v <- n match
            case Literal(IntConstant(i)) => Some(i)
            case other =>
              report.error(s"$method requires an integer literal", other.pos)
              None
        yield Rel.Repartition(input, v, shuffle = method == "repartition")

      case Apply(Select(receiver, "repartitionBy"), args) =>
        for
          input <- rel(receiver, env)
          es    <- traverse(flattenVarargs(args))(ex(_, env))
        yield Rel.RepartitionByExpression(input, es, None)

      case Apply(Select(receiver, "dropNa"), args) =>
        for
          input <- rel(receiver, env)
          cs    <- traverse(flattenVarargs(args))(str(_, "dropNa column"))
        yield Rel.DropNa(input, cs)

      case Apply(Select(receiver, "fillNa"), valueArg :: cols) =>
        for
          input <- rel(receiver, env)
          v <- ex(valueArg, env).flatMap {
            case Ex.Lit(value) => Some(value)
            case _ =>
              report.error("fillNa value must be a lit(...)", valueArg.pos)
              None
          }
          cs <- traverse(flattenVarargs(cols))(str(_, "fillNa column"))
        yield Rel.FillNa(input, cs, v)

      case Apply(Select(receiver, "describe"), args) =>
        for
          input <- rel(receiver, env)
          cs    <- traverse(flattenVarargs(args))(str(_, "describe column"))
        yield Rel.Describe(input, cs)

      case Apply(Select(receiver, "summary"), args) =>
        for
          input <- rel(receiver, env)
          ss    <- traverse(flattenVarargs(args))(str(_, "summary statistic"))
        yield Rel.Summary(input, ss)

      case Apply(Select(receiver, method @ ("crosstab" | "cov" | "corr")), List(a, b)) =>
        for
          input <- rel(receiver, env)
          c1    <- str(a, s"$method column")
          c2    <- str(b, s"$method column")
        yield method match
          case "crosstab" => Rel.Crosstab(input, c1, c2)
          case "cov"      => Rel.Cov(input, c1, c2)
          case "corr"     => Rel.Corr(input, c1, c2)

      case Apply(Select(receiver, "freqItems"), args) =>
        for
          input <- rel(receiver, env)
          cs    <- traverse(flattenVarargs(args))(str(_, "freqItems column"))
        yield Rel.FreqItems(input, cs)

      case Apply(Apply(Select(receiver, "unpivot"), ids), List(varArg, valArg)) =>
        for
          input  <- rel(receiver, env)
          idCols <- traverse(flattenVarargs(ids))(ex(_, env))
          varCol <- str(varArg, "unpivot variable column name")
          valCol <- str(valArg, "unpivot value column name")
        yield Rel.Unpivot(input, idCols, Nil, varCol, valCol)

      case Apply(Select(receiver, "transpose"), args) =>
        for
          input <- rel(receiver, env)
          es    <- traverse(flattenVarargs(args))(ex(_, env))
        yield Rel.Transpose(input, es)

      case Apply(Select(receiver, "replaceValues"), oldArg :: newArg :: cols) =>
        for
          input <- rel(receiver, env)
          o <- ex(oldArg, env).flatMap {
            case Ex.Lit(v) => Some(v)
            case _ => report.error("replaceValues old value must be a lit(...)", oldArg.pos); None
          }
          n <- ex(newArg, env).flatMap {
            case Ex.Lit(v) => Some(v)
            case _ => report.error("replaceValues new value must be a lit(...)", newArg.pos); None
          }
          cs <- traverse(flattenVarargs(cols))(str(_, "replaceValues column"))
        yield Rel.Replace(input, cs, List((o, n)))

      case Apply(Select(receiver, "observe"), nameArg :: metrics) =>
        for
          input <- rel(receiver, env)
          name  <- str(nameArg, "observe name")
          ms    <- traverse(flattenVarargs(metrics))(ex(_, env))
        yield Rel.CollectMetrics(input, name, ms)

      case Apply(Select(receiver, "withColumnRenamed"), List(fromArg, toArg)) =>
        for
          input <- rel(receiver, env)
          from  <- str(fromArg, "withColumnRenamed existing name")
          to    <- str(toArg, "withColumnRenamed new name")
        yield input match
          case Rel.WithColumnsRenamed(inner, renames) =>
            Rel.WithColumnsRenamed(inner, renames :+ (from -> to))
          case other => Rel.WithColumnsRenamed(other, List(from -> to))

      // withColumnsRenamed(Map("from" -> "to", ...)) — Spark's Map signature
      case Apply(Select(receiver, "withColumnsRenamed"), List(mapArg)) =>
        for
          input   <- rel(receiver, env)
          entries <- mapLiteralEntries(mapArg).flatMap(traverse(_)(renameEntry))
        yield Rel.WithColumnsRenamed(input, entries)

      // withColumnsRenamed(Seq("a","b"), Seq("x","y")) — parallel name lists
      case Apply(Select(receiver, "withColumnsRenamed"), List(fromsArg, tosArg)) =>
        for
          input <- rel(receiver, env)
          froms <- seqLiteralStrings(fromsArg, "withColumnsRenamed source")
          tos   <- seqLiteralStrings(tosArg, "withColumnsRenamed target")
          pairs <-
            if froms.sizeIs == tos.size then Some(froms.zip(tos))
            else
              report.error("withColumnsRenamed name lists must be the same length", term.pos)
              None
        yield Rel.WithColumnsRenamed(input, pairs)

      case Apply(Select(receiver, "orderBy" | "sort"), args) =>
        for
          input <- rel(receiver, env)
          keys  <- traverse(flattenVarargs(args))(sortKey(_, env))
        yield Rel.Sort(input, keys)

      case Apply(Select(receiver, method @ ("limit" | "offset" | "tail")), List(n)) =>
        for
          input <- rel(receiver, env)
          v <- n match
            case Literal(IntConstant(i)) => Some(i)
            case other =>
              report.error(s"$method requires an integer literal; got: ${other.show}", other.pos)
              None
        yield method match
          case "limit"  => Rel.Limit(input, v)
          case "offset" => Rel.Offset(input, v)
          case "tail"   => Rel.Tail(input, v)

      case other =>
        report.error(
          s"Unsupported flow-body construct: ${other.show}. The flow language supports " +
            "stream.table/stream.source/read.table, select/where/groupBy.agg/join/orderBy/limit, " +
            "and val bindings of relations or expressions.",
          other.pos,
        )
        None

    /** A `val` rhs may be a relation, an expression, or a typed-column
      * handle — routed by static type.
      */
    def bind(rhs: Term, env: Env): Option[Bound] =
      if rhs.tpe <:< TypeRepr.of[TypedCols[?]] then Some(ColsHandle)
      else if rhs.tpe <:< TypeRepr.of[ExArg] then ex(rhs, env)
      else rel(rhs, env)

    rel(body.underlyingArgument, Map.empty)
