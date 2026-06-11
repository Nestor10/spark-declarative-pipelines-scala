package dev.sdp.dsl

/** The Spark `functions` surface — the analog of
  * `org.apache.spark.sql.functions`. Import it alongside the DSL:
  *
  * {{{
  * import dev.sdp.dsl.*            // entry points, spark, col, lit, operators
  * import dev.sdp.dsl.functions.* // the function library
  *
  * spark.table("orders")
  *   .groupBy(col("state"))
  *   .agg(count(col("id")), sum(col("amount")).as("total"))
  *   .withColumn("rank", row_number().over(Window.partitionBy(col("state"))))
  * }}}
  *
  * Everything desugars to `Ex.Fn(wireName, args)` — names bind in the
  * *server's* analyzer (no client registry to go stale), so these stubs add
  * typing/arity only. Higher-order functions take *real Scala lambdas*
  * (`transform(col("xs"), x => x * lit(2))`), matching Spark's signatures —
  * the author's parameter names travel to the wire.
  *
  * Wire-name mismatches follow Spark's own `functions.scala` (authority
  * rule): `pow` → `power`, `mean` → `avg`. Anything not stubbed here is one
  * `fn("name", args*)` away — same wire bytes.
  *
  * The two import surfaces are **disjoint** by design: the library lives only
  * here, while `col`/`lit`/`expr`/`fn`/`star` and the subquery combinators
  * (`exists`/`scalar`) live in `dev.sdp.dsl` — so importing both is
  * unambiguous, exactly like `import spark.implicits._` next to
  * `import functions._`.
  */
object functions:

  // ---- aggregates -----------------------------------------------------
  def count(e: ExArg): ExArg        = ExArg()
  def sum(e: ExArg): ExArg          = ExArg()
  def avg(e: ExArg): ExArg          = ExArg()
  def mean(e: ExArg): ExArg         = ExArg() // wire: avg
  def min(e: ExArg): ExArg          = ExArg()
  def max(e: ExArg): ExArg          = ExArg()
  def first(e: ExArg): ExArg        = ExArg()
  def last(e: ExArg): ExArg         = ExArg()
  def collect_list(e: ExArg): ExArg = ExArg()
  def collect_set(e: ExArg): ExArg  = ExArg()

  // ---- window functions -----------------------------------------------
  def row_number(): ExArg                                = ExArg()
  def rank(): ExArg                                      = ExArg()
  def dense_rank(): ExArg                                = ExArg()
  def ntile(n: Int): ExArg                               = ExArg()
  def lag(e: ExArg, offset: Int): ExArg                  = ExArg()
  def lag(e: ExArg, offset: Int, default: ExArg): ExArg  = ExArg()
  def lead(e: ExArg, offset: Int): ExArg                 = ExArg()
  def lead(e: ExArg, offset: Int, default: ExArg): ExArg = ExArg()

  // ---- strings --------------------------------------------------------
  def upper(e: ExArg): ExArg                                   = ExArg()
  def lower(e: ExArg): ExArg                                   = ExArg()
  def trim(e: ExArg): ExArg                                    = ExArg()
  def length(e: ExArg): ExArg                                  = ExArg()
  def concat(exprs: ExArg*): ExArg                             = ExArg()
  def concat_ws(sep: String, exprs: ExArg*): ExArg             = ExArg()
  def substring(e: ExArg, pos: Int, len: Int): ExArg           = ExArg()
  def split(e: ExArg, pattern: String): ExArg                  = ExArg()
  def regexp_replace(e: ExArg, pattern: String, replacement: String): ExArg = ExArg()

  // ---- dates & times --------------------------------------------------
  def to_date(e: ExArg): ExArg                   = ExArg()
  def to_date(e: ExArg, fmt: String): ExArg      = ExArg()
  def to_timestamp(e: ExArg): ExArg              = ExArg()
  def to_timestamp(e: ExArg, fmt: String): ExArg = ExArg()
  def date_trunc(format: String, e: ExArg): ExArg = ExArg()
  def year(e: ExArg): ExArg                      = ExArg()
  def month(e: ExArg): ExArg                     = ExArg()
  def dayofmonth(e: ExArg): ExArg                = ExArg()
  def hour(e: ExArg): ExArg                      = ExArg()
  def minute(e: ExArg): ExArg                    = ExArg()
  def second(e: ExArg): ExArg                    = ExArg()
  def current_date(): ExArg                      = ExArg()
  def current_timestamp(): ExArg                 = ExArg()
  def date_add(e: ExArg, days: Int): ExArg       = ExArg()
  def date_sub(e: ExArg, days: Int): ExArg       = ExArg()
  def datediff(end: ExArg, start: ExArg): ExArg  = ExArg()

  // ---- math -----------------------------------------------------------
  def abs(e: ExArg): ExArg                = ExArg()
  def round(e: ExArg): ExArg              = ExArg()
  def round(e: ExArg, scale: Int): ExArg  = ExArg()
  def floor(e: ExArg): ExArg              = ExArg()
  def ceil(e: ExArg): ExArg               = ExArg()
  def sqrt(e: ExArg): ExArg               = ExArg()
  def pow(l: ExArg, r: ExArg): ExArg      = ExArg() // wire: power
  def exp(e: ExArg): ExArg                = ExArg()
  def log(e: ExArg): ExArg                = ExArg()
  def greatest(exprs: ExArg*): ExArg      = ExArg()
  def least(exprs: ExArg*): ExArg         = ExArg()

  // ---- null / conditional ---------------------------------------------
  def coalesce(exprs: ExArg*): ExArg = ExArg()
  def isnull(e: ExArg): ExArg        = ExArg()
  def isnan(e: ExArg): ExArg         = ExArg()

  /** `when(cond, value)` — chain `.when(...)` and finish with `.otherwise(...)`
    * exactly as in Spark. Wire shape verified against Spark's own converter:
    * `UnresolvedFunction("when", [c1, v1, c2, v2, ..., else])`.
    */
  def when(condition: ExArg, value: ExArg): ExArg = ExArg()

  // ---- complex types --------------------------------------------------
  def struct(exprs: ExArg*): ExArg               = ExArg()
  def array(exprs: ExArg*): ExArg                = ExArg()
  def map(exprs: ExArg*): ExArg                  = ExArg()
  def explode(e: ExArg): ExArg                   = ExArg()
  def explode_outer(e: ExArg): ExArg             = ExArg()
  def size(e: ExArg): ExArg                      = ExArg()
  def element_at(e: ExArg, key: ExArg): ExArg    = ExArg()
  def array_contains(e: ExArg, value: ExArg): ExArg = ExArg()
  def map_keys(e: ExArg): ExArg                  = ExArg()
  def map_values(e: ExArg): ExArg                = ExArg()

  // ---- higher-order functions (real Scala lambdas) --------------------
  def transform(column: ExArg, f: ExArg => ExArg): ExArg          = ExArg()
  def filter(column: ExArg, f: ExArg => ExArg): ExArg             = ExArg()
  def forall(column: ExArg, f: ExArg => ExArg): ExArg             = ExArg()
  def aggregate(column: ExArg, initialValue: ExArg, merge: (ExArg, ExArg) => ExArg): ExArg = ExArg()
  def zip_with(left: ExArg, right: ExArg, f: (ExArg, ExArg) => ExArg): ExArg = ExArg()
  // NB: no array-HOF `exists` here — `exists(relation)` is the subquery
  // combinator in `dev.sdp.dsl`; for the array HOF use `fn("exists", col, lam)`.

  // ---- misc -----------------------------------------------------------
  def hash(exprs: ExArg*): ExArg                = ExArg()
  def md5(e: ExArg): ExArg                      = ExArg()
  def sha2(e: ExArg, numBits: Int): ExArg       = ExArg()
  def monotonically_increasing_id(): ExArg      = ExArg()
  def rand(): ExArg                             = ExArg()
  def rand(seed: Long): ExArg                   = ExArg()
