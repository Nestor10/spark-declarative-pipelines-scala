package dev.sdp.core.algebra

import dev.sdp.core.*
import zio.test.*

/** Property-based codec tests (review item 7).
  *
  * The example-based specs check the constructs we thought of. These check the
  * LAW — `parse(render(t)) == t` — over a bounded generator of arbitrary trees
  * with deliberately hostile atoms (separators, percent signs, newlines,
  * parentheses, unicode), because every atom in the s-expression dialect is
  * percent-encoded precisely so those cannot break the grammar.
  *
  * Two review findings are covered by construction here:
  *   - item 4 (codec totality): a fuzz property mutates rendered strings and
  *     asserts `parse` returns an `Either` and NEVER throws;
  *   - item 3 (nested lambdas): a property over nesting depth asserts every
  *     lambda in a nested HOF binds a distinct variable.
  *
  * NaN is decided explicitly, not left to `toString`: non-finite doubles are
  * REJECTED at both ends (see `RelCodec.finiteOrError`), so the law holds for
  * every tree the DSL can build. The tests below pin that choice.
  */
object RelCodecPropertySpec extends ZIOSpecDefault:

  // ------------------------------------------------------------------
  // generators
  // ------------------------------------------------------------------

  /** Atoms that would break a naive (unencoded) dialect: the field separator,
    * percent escapes, whitespace, the s-expression delimiters, unicode. */
  private val hostile: Gen[Any, String] = Gen.oneOf(
    Gen.const(""),
    Gen.const("|"),
    Gen.const("%"),
    Gen.const("%zz"),
    Gen.const("%7C"),
    Gen.const("+"),
    Gen.const(" "),
    Gen.const("a b"),
    Gen.const("\n"),
    Gen.const("\t"),
    Gen.const("("),
    Gen.const(")"),
    Gen.const("(read t batch)"),
    Gen.const("ünïcødé"),
    Gen.const("日本語"),
    Gen.const("emoji 🎉"),
    Gen.const("'quoted'"),
    Gen.const("\\backslash"),
    Gen.const("SELECT * FROM t WHERE a = 'x|y'"),
    Gen.alphaNumericStringBounded(1, 8),
    Gen.stringBounded(1, 6)(Gen.elements('a', 'B', '_', '.', '-', '|', '%', ' ', '(', ')', '+')),
  )

  /** Non-empty atoms, for positions where a name must exist (dataset ids). */
  private val hostileName: Gen[Any, String] = hostile.filter(_.nonEmpty)

  private val colType: Gen[Any, ColType] = Gen.elements(
    ColType.Bool,
    ColType.I32,
    ColType.I64,
    ColType.F64,
    ColType.Str,
    ColType.Timestamp,
    ColType.Date,
    ColType.Unknown,
  )

  /** Only FINITE doubles — non-finite values are not representable (see the
    * NaN suite below). */
  private val finiteDouble: Gen[Any, Double] = Gen.oneOf(
    Gen.const(0.0),
    Gen.const(-0.0),
    Gen.const(1.0),
    Gen.const(0.25),
    Gen.const(-3.5e-7),
    Gen.const(Double.MaxValue),
    Gen.const(Double.MinPositiveValue),
    Gen.double(-1e9, 1e9),
  )

  private val litValue: Gen[Any, LitValue] = Gen.oneOf(
    Gen.boolean.map(LitValue.Bool(_)),
    Gen.int(-1000, 1000).map(LitValue.I32(_)),
    Gen.long(-1000000L, 1000000L).map(LitValue.I64(_)),
    finiteDouble.map(LitValue.F64(_)),
    hostile.map(LitValue.Str(_)),
    Gen.const(LitValue.Null),
  )

  private val schema: Gen[Any, List[(String, ColType)]] =
    Gen.listOfBounded(0, 3)(for n <- hostile; t <- colType yield (n, t))

  private val options: Gen[Any, Map[String, String]] =
    Gen.listOfBounded(0, 3)(for k <- hostile; v <- hostile yield (k, v)).map(_.toMap)

  private def exGen(depth: Int): Gen[Any, Ex] =
    val leaves: Gen[Any, Ex] = Gen.oneOf(
      hostile.map(Ex.Col(_)),
      litValue.map(Ex.Lit(_)),
      hostile.map(Ex.ExprString(_)),
      Gen.option(hostile).map(Ex.Star(_)),
      hostile.map(Ex.ColRegex(_)),
      hostile.map(Ex.LamVar(_)),
    )
    if depth <= 0 then leaves
    else
      Gen.suspend(
        Gen.oneOf(
          leaves,
          for
            name     <- hostile
            args     <- Gen.listOfBounded(0, 2)(exGen(depth - 1))
            distinct <- Gen.boolean
          yield Ex.Fn(name, args, distinct),
          for e <- exGen(depth - 1); n <- hostile yield Ex.Alias(e, n),
          for e <- exGen(depth - 1); t <- colType yield Ex.Cast(e, t),
          for c <- exGen(depth - 1); k <- exGen(depth - 1) yield Ex.ExtractValue(c, k),
          for n <- hostile; as <- Gen.listOfBounded(0, 2)(exGen(depth - 1)) yield Ex.CallFn(n, as),
          for ps <- Gen.listOfBounded(1, 2)(hostile); b <- exGen(depth - 1) yield Ex.Lam(ps, b),
          windowGen(depth - 1),
          subqueryGen(depth - 1),
        )
      )

  private def sortKeyGen(depth: Int): Gen[Any, SortKey] =
    for
      e  <- exGen(depth)
      d  <- Gen.boolean
      nf <- Gen.option(Gen.boolean)
    yield SortKey(e, d, nf)

  private def frameBoundary(depth: Int): Gen[Any, FrameBoundary] = Gen.oneOf(
    Gen.const(FrameBoundary.CurrentRow),
    Gen.const(FrameBoundary.Unbounded),
    exGen(depth).map(FrameBoundary.Value(_)),
  )

  private def windowGen(depth: Int): Gen[Any, Ex] =
    for
      fn    <- exGen(depth)
      part  <- Gen.listOfBounded(0, 2)(exGen(depth))
      order <- Gen.listOfBounded(0, 2)(sortKeyGen(depth))
      frame <- Gen.option(
        for
          rows  <- Gen.boolean
          lower <- frameBoundary(depth)
          upper <- frameBoundary(depth)
        yield WindowFrame(rows, lower, upper)
      )
    yield Ex.Window(fn, part, order, frame)

  private def subqueryGen(depth: Int): Gen[Any, Ex] =
    for
      rel <- relGen(depth)
      kind <- Gen.oneOf(
        Gen.const(SubqueryKind.Scalar),
        Gen.const(SubqueryKind.Exists),
        Gen.listOfBounded(1, 2)(exGen(0)).map(SubqueryKind.In(_)),
      )
    yield Ex.Subquery(rel, kind)

  private val dataSourceGen: Gen[Any, Rel] =
    for
      format <- hostile
      opts   <- options
      stream <- Gen.boolean
      sch    <- schema
      ddl    <- Gen.option(hostile)
    yield Rel.DataSource(format, opts, stream, sch, ddl)

  /** Rows must match the schema by arity — the codec renders them positionally. */
  private val localDataGen: Gen[Any, Rel] =
    for
      sch  <- Gen.listOfBounded(1, 3)(for n <- hostile; t <- colType yield (n, t))
      rows <- Gen.listOfBounded(0, 3)(Gen.listOfN(sch.size)(litValue))
    yield Rel.LocalData(sch, rows)

  private val joinType: Gen[Any, JoinType] = Gen.elements(
    JoinType.Inner,
    JoinType.FullOuter,
    JoinType.LeftOuter,
    JoinType.RightOuter,
    JoinType.LeftAnti,
    JoinType.LeftSemi,
    JoinType.Cross,
  )

  private def relGen(depth: Int): Gen[Any, Rel] =
    val leaves: Gen[Any, Rel] = Gen.oneOf(
      for n <- hostile; s <- Gen.boolean yield Rel.NamedTable(n, s),
      hostile.map(Rel.Sql(_)),
      for
        s  <- Gen.long(-100L, 100L)
        e  <- Gen.long(-100L, 100L)
        st <- Gen.long(1L, 10L)
      yield Rel.Range(s, e, st),
      schema.map(Rel.LocalRelation(_)),
      Gen.elements(
        Rel.Catalog(CatalogOp.CurrentDatabase),
        Rel.Catalog(CatalogOp.ListDatabases),
        Rel.Catalog(CatalogOp.ListTables),
      ),
      localDataGen,
      dataSourceGen,
      for n <- hostile; as <- Gen.listOfBounded(0, 2)(exGen(0)) yield Rel.Tvf(n, as),
    )
    if depth <= 0 then leaves
    else
      Gen.suspend(
        Gen.oneOf(
          leaves,
          for r <- relGen(depth - 1); cs <- Gen.listOfBounded(0, 3)(exGen(1))
          yield Rel.Project(r, cs),
          for r <- relGen(depth - 1); c <- exGen(1) yield Rel.Filter(r, c),
          for
            l  <- relGen(depth - 1)
            rr <- relGen(depth - 1)
            c  <- Gen.option(exGen(1))
            jt <- joinType
          yield Rel.Join(l, rr, c, jt),
          for
            r  <- relGen(depth - 1)
            gs <- Gen.listOfBounded(0, 2)(exGen(1))
            as <- Gen.listOfBounded(0, 2)(exGen(1))
          yield Rel.Aggregate(r, gs, as),
          for r <- relGen(depth - 1); ks <- Gen.listOfBounded(0, 2)(sortKeyGen(1))
          yield Rel.Sort(r, ks),
          for r <- relGen(depth - 1); n <- Gen.int(0, 1000) yield Rel.Limit(r, n),
          for r <- relGen(depth - 1); n <- Gen.int(0, 1000) yield Rel.Offset(r, n),
          for r <- relGen(depth - 1); n <- Gen.int(0, 1000) yield Rel.Tail(r, n),
          for r <- relGen(depth - 1); cs <- Gen.listOfBounded(0, 2)(hostile)
          yield Rel.Deduplicate(r, cs),
          for r <- relGen(depth - 1); cs <- Gen.listOfBounded(0, 2)(hostile) yield Rel.Drop(r, cs),
          for
            l   <- relGen(depth - 1)
            rr  <- relGen(depth - 1)
            op  <- Gen.elements(SetOpType.Union, SetOpType.Intersect, SetOpType.Except)
            all <- Gen.boolean
          yield Rel.SetOp(l, rr, op, all),
          for r <- relGen(depth - 1); a <- hostile yield Rel.SubqueryAlias(r, a),
          for r <- relGen(depth - 1); ns <- Gen.listOfBounded(0, 3)(hostile) yield Rel.ToDF(r, ns),
          for
            r  <- relGen(depth - 1)
            cs <- Gen.listOfBounded(0, 2)(for n <- hostile; e <- exGen(1) yield (n, e))
          yield Rel.WithColumns(r, cs),
          for
            r  <- relGen(depth - 1)
            rs <- Gen.listOfBounded(0, 2)(for a <- hostile; b <- hostile yield (a, b))
          yield Rel.WithColumnsRenamed(r, rs),
          for
            r <- relGen(depth - 1)
            f <- finiteDouble
            s <- Gen.option(Gen.long(-10L, 10L))
          yield Rel.Sample(r, f, s),
          for
            r  <- relGen(depth - 1)
            n  <- hostile
            ps <- Gen.listOfBounded(0, 2)(exGen(0))
          yield Rel.Hint(r, n, ps),
          for
            r  <- relGen(depth - 1)
            n  <- Gen.int(1, 64)
            sh <- Gen.boolean
          yield Rel.Repartition(r, n, sh),
          for
            r  <- relGen(depth - 1)
            es <- Gen.listOfBounded(0, 2)(exGen(0))
            n  <- Gen.option(Gen.int(1, 64))
          yield Rel.RepartitionByExpression(r, es, n),
          for r <- relGen(depth - 1); cs <- Gen.listOfBounded(0, 2)(hostile) yield Rel.DropNa(r, cs),
          for
            r  <- relGen(depth - 1)
            cs <- Gen.listOfBounded(0, 2)(hostile)
            v  <- litValue
          yield Rel.FillNa(r, cs, v),
          for
            r    <- relGen(depth - 1)
            ids  <- Gen.listOfBounded(0, 2)(exGen(0))
            vals <- Gen.listOfBounded(0, 2)(exGen(0))
            vc   <- hostile
            valc <- hostile
          yield Rel.Unpivot(r, ids, vals, vc, valc),
          for r <- relGen(depth - 1); ic <- Gen.listOfBounded(0, 2)(exGen(0))
          yield Rel.Transpose(r, ic),
          for
            r    <- relGen(depth - 1)
            cs   <- Gen.listOfBounded(0, 2)(hostile)
            reps <- Gen.listOfBounded(0, 2)(for a <- litValue; b <- litValue yield (a, b))
          yield Rel.Replace(r, cs, reps),
          for
            r  <- relGen(depth - 1)
            c  <- exGen(0)
            fs <- Gen.listOfBounded(0, 2)(for v <- litValue; f <- finiteDouble yield (v, f))
            s  <- Gen.option(Gen.long(-10L, 10L))
          yield Rel.SampleBy(r, c, fs, s),
          for
            r  <- relGen(depth - 1)
            cs <- Gen.listOfBounded(0, 2)(hostile)
            ps <- Gen.listOfBounded(0, 2)(Gen.double(0.0, 1.0))
            re <- Gen.double(0.0, 1.0)
          yield Rel.ApproxQuantile(r, cs, ps, re),
          for
            r  <- relGen(depth - 1)
            n  <- hostile
            ms <- Gen.listOfBounded(0, 2)(exGen(0))
          yield Rel.CollectMetrics(r, n, ms),
          for
            l  <- relGen(depth - 1)
            rr <- relGen(depth - 1)
            la <- exGen(0)
            ra <- exGen(0)
          yield Rel.AsOfJoin(l, rr, la, ra, "inner", "backward", true, None),
          for
            l  <- relGen(depth - 1)
            rr <- relGen(depth - 1)
            c  <- Gen.option(exGen(0))
          yield Rel.LateralJoin(l, rr, c, JoinType.Inner),
          for
            r <- relGen(depth - 1)
            f <- Gen.elements(ParseFormat.Csv, ParseFormat.Json)
            o <- options
          yield Rel.Parse(r, f, o),
          for r <- relGen(depth - 1); s <- schema yield Rel.ToSchema(r, s),
          for
            r <- relGen(depth - 1)
            n <- Gen.int(0, 20)
            t <- Gen.int(0, 20)
            v <- Gen.boolean
          yield Rel.ShowString(r, n, t, v),
          for
            r <- relGen(depth - 1)
            n <- Gen.int(0, 20)
            t <- Gen.int(0, 20)
          yield Rel.HtmlString(r, n, t),
          for r <- relGen(depth - 1); cs <- Gen.listOfBounded(0, 2)(hostile)
          yield Rel.Describe(r, cs),
          for r <- relGen(depth - 1); ss <- Gen.listOfBounded(0, 2)(hostile)
          yield Rel.Summary(r, ss),
          for r <- relGen(depth - 1); a <- hostile; b <- hostile yield Rel.Crosstab(r, a, b),
          for r <- relGen(depth - 1); a <- hostile; b <- hostile yield Rel.Cov(r, a, b),
          for r <- relGen(depth - 1); a <- hostile; b <- hostile yield Rel.Corr(r, a, b),
          for r <- relGen(depth - 1); cs <- Gen.listOfBounded(0, 2)(hostile)
          yield Rel.FreqItems(r, cs),
        )
      )

  private val trees: Gen[Any, Rel] = relGen(3)

  /** The ONLY form `RelCodec.render` emits for a `Rel.NamedTable`. Every atom
    * in the dialect is percent-encoded (spaces become `+`, parens `%28`/`%29`),
    * so a hostile string like `"(read t batch)"` can never forge one — which is
    * what makes the render an INDEPENDENT oracle for the traversal below. */
  private val ReadForm = """\(read (\S+) (?:stream|batch)\)""".r

  private def decodeAtom(atom: String): String =
    if atom == "~" then "" // LineCodec's reserved empty-atom sentinel
    else java.net.URLDecoder.decode(atom, java.nio.charset.StandardCharsets.UTF_8)

  private val nodeGen: Gen[Any, PipelineNode] = Gen.oneOf(
    for i <- hostileName; f <- hostile yield PipelineNode.Table(i, f),
    for i <- hostileName; f <- hostile yield PipelineNode.StreamingTable(i, f),
    for i <- hostileName; s <- hostile yield PipelineNode.MaterializedView(i, s),
    for i <- hostileName; s <- hostile yield PipelineNode.TemporaryView(i, s),
    hostileName.map(PipelineNode.ExternalTable(_)),
  )

  // ------------------------------------------------------------------
  // the laws
  // ------------------------------------------------------------------

  def spec = suite("codec round-trip properties")(
    test("law: RelCodec.parse(render(rel)) == Right(rel)") {
      check(trees) { rel =>
        assertTrue(RelCodec.parse(RelCodec.render(rel)) == Right(rel))
      }
    },
    test("law: RelCodec.parseEx(renderEx(ex)) == Right(ex)") {
      check(exGen(3)) { ex =>
        assertTrue(RelCodec.parseEx(RelCodec.renderEx(ex)) == Right(ex))
      }
    },
    test("render is a pure function of structure (equal trees, equal bytes)") {
      check(trees) { rel =>
        assertTrue(RelCodec.render(rel) == RelCodec.render(rel))
      }
    },
    test("law: a v2/v3 flow line round-trips through the line codec") {
      check(hostile, hostile, trees, Gen.boolean) { (name, target, rel, once) =>
        val flow = Flow(name, target, FlowDetails.WriteRelation(rel), once)
        assertTrue(
          LineCodec.parseLine(LineCodec.renderFlow(flow)) ==
            Right(LineCodec.ParsedLine.FlowLine(flow))
        )
      }
    },
    test("law: an AUTO CDC flow line round-trips") {
      check(hostile, hostile, exGen(1), Gen.listOfBounded(1, 2)(exGen(1))) {
        (name, source, seq, keys) =>
          val flow = Flow(
            name,
            name,
            FlowDetails.AutoCdc(source = source, keys = keys, sequenceBy = seq),
            once = true,
          )
          assertTrue(
            LineCodec.parseLine(LineCodec.renderFlow(flow)) ==
              Right(LineCodec.ParsedLine.FlowLine(flow))
          )
      }
    },
    test("law: GraphFragment.parse(render(f)) recovers the fragment") {
      check(
        Gen.listOfBounded(0, 3)(nodeGen),
        Gen.listOfBounded(0, 3)(for a <- hostile; b <- hostile yield (a, b)),
        Gen.listOfBounded(0, 2)(for n <- hostile; r <- trees yield (n, r)),
      ) { (nodes, edges, flows) =>
        val fragment = GraphFragment(
          nodes,
          edges.map((f, t) => DependencyEdge(f, t)).toSet,
          flows.map((n, r) => Flow(n, n, r)),
        )
        val parsed = GraphFragment.parse(GraphFragment.render(fragment))
        assertTrue(
          parsed.map(_.nodes.sortBy(_.id)) == Right(fragment.nodes.sortBy(_.id)),
          parsed.map(_.edges) == Right(fragment.edges),
          parsed.map(_.flows.sortBy(f => (f.target, f.name))) ==
            Right(fragment.flows.sortBy(f => (f.target, f.name))),
        )
      }
    },
    test("law: PipelineManifest.parse(m.render) == Right(m)") {
      check(Gen.listOfBounded(1, 3)(for n <- hostileName; r <- trees yield (n, r)), Gen.boolean) {
        (flows, once) =>
          val named = flows.distinctBy(_._1)
          val nodes = named.map((n, _) => PipelineNode.Table(n, "delta"))
          val manifest = PipelineManifest.fromGraphAndFlows(
            PipelineGraph(nodes.map(n => n.id -> n).toMap, Set.empty),
            named.map((n, rel) => Flow(n, n, FlowDetails.WriteRelation(rel), once)),
          )
          assertTrue(PipelineManifest.parse(manifest.render) == Right(manifest))
      }
    },
    suite("NaN: rejected, by decision")(
      test("the DSL refuses a non-finite literal, fraction or inline cell") {
        import dev.sdp.dsl.*
        def rejected(thunk: => Any): Boolean =
          try { val _ = thunk; false }
          catch case e: IllegalArgumentException => e.getMessage.contains("must be finite")
        assertTrue(
          rejected(lit(Double.NaN)),
          rejected(lit(Double.PositiveInfinity)),
          rejected(lit(Double.NegativeInfinity)),
          rejected(spark.table("t").sample(Double.NaN)),
          rejected(spark.createDataFrame(Seq(1.0, Double.NaN)).toDF("x").rel),
          // …and a finite one is still perfectly fine
          lit(0.25).ex == Ex.Lit(LitValue.F64(0.25)),
        )
      },
      test("the codec refuses a smuggled non-finite value, with a readable error") {
        val nan = RelCodec.parse("(project (read t batch) (lit f64 NaN))")
        assertTrue(
          nan.isLeft,
          nan.swap.exists(_.contains("must be finite")),
          RelCodec.parse("(project (read t batch) (lit f64 Infinity))").isLeft,
          RelCodec.parse("(sample (read t batch) NaN noseed)").isLeft,
          RelCodec.parseEx("(lit f64 -Infinity)").isLeft,
          // the finite form parses
          RelCodec.parseEx("(lit f64 0.25)") == Right(Ex.Lit(LitValue.F64(0.25))),
        )
      },
      test("finiteOrError is the single decision point") {
        assertTrue(
          RelCodec.finiteOrError("x", 1.5) == Right(1.5),
          RelCodec.finiteOrError("x", Double.NaN).isLeft,
          RelCodec.finiteOrError("x", Double.PositiveInfinity).isLeft,
        )
      },
    ),
    suite("traversal totality (P3.1)")(
      test("law: Flow.reads sees exactly the (read …) forms the codec renders") {
        // The traversal (AlgebraShape) and the renderer are two independent
        // exhaustive matches over Rel. If either silently drops a subtree —
        // which is precisely what the old `case _ => Nil` catch-alls allowed —
        // they disagree here. Subquery-embedded reads are included on both
        // sides: `render` descends into `(subq …)`, and so must lineage.
        check(trees) { rel =>
          val rendered   = RelCodec.render(rel)
          val fromRender = ReadForm.findAllMatchIn(rendered).map(m => decodeAtom(m.group(1))).toSet
          assertTrue(Flow.reads(rel) == fromRender)
        }
      },
      test("allRels is the structural pre-order and is closed under children") {
        check(trees) { rel =>
          val all = Flow.allRels(rel)
          assertTrue(
            all.headOption.contains(rel),
            all.forall(node => AlgebraShape.of(node).children.forall(all.contains)),
          )
        }
      },
    ),
    suite("totality under mutation (item 4 regression net)")(
      test("no mutation of a rendered tree can make parse THROW") {
        val mutate: Gen[Any, (String, String => String)] = Gen.elements(
          ("prefix-escape", (s: String) => s.replaceFirst("""\(""", "%zz(")),
          ("suffix-escape", (s: String) => s + "%zz"),
          ("truncate-half", (s: String) => s.take(s.length / 2)),
          ("drop-head", (s: String) => s.drop(1)),
          ("strip-closers", (s: String) => s.replace(")", "")),
          ("spaces-to-percent", (s: String) => s.replace(" ", "%")),
          ("break-tag", (s: String) => s.replaceFirst("read", "%a")),
          ("upcase", (s: String) => s.toUpperCase),
        )
        check(trees, mutate) { (rel, mutation) =>
          val (label, f) = mutation
          val text       = f(RelCodec.render(rel))
          val threw =
            try { val _ = RelCodec.parse(text); false }
            catch case _: Throwable => true
          assertTrue(!threw) ?? s"parse threw ($label) on: $text"
        }
      },
      test("no mutation of a rendered flow line can make parseLine THROW") {
        check(trees, Gen.int(0, 3)) { (rel, which) =>
          val line = LineCodec.renderFlow(Flow("f", "t", rel))
          val text = which match
            case 0 => line + "%zz"
            case 1 => line.replace("|", "|%zz")
            case 2 => line.take(line.length - 3)
            case _ => line.replace("%", "%z")
          val threw =
            try { val _ = LineCodec.parseLine(text); false }
            catch case _: Throwable => true
          assertTrue(!threw) ?? s"parseLine threw on: $text"
        }
      },
    ),
    test("nested HOF lambdas bind distinct variables at every depth (item 3)") {
      import dev.sdp.dsl.*
      // genuinely NESTED: each level's HOF lives inside the previous lambda's
      // body and reads that lambda's variable
      def nest(depth: Int, outer: Column): Column =
        if depth <= 0 then outer else functions.transform(outer, x => nest(depth - 1, x))
      def params(ex: Ex): List[List[String]] = ex match
        case Ex.Lam(ps, body)  => ps :: params(body)
        case Ex.Fn(_, args, _) => args.flatMap(params)
        case Ex.Alias(e, _)    => params(e)
        case _                 => Nil
      check(Gen.int(1, 6)) { depth =>
        val all = params(nest(depth, col("xs")).ex)
        assertTrue(
          all.size == depth,
          all.flatten.distinct.size == all.flatten.size,
        )
      }
    },
  )
