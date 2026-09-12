package dev.sdp.core

import dev.sdp.core.algebra.{Ex, RelCodec}

/** Canonical serialization of [[FlowDetails]] for manifest format v3.
  *
  * A v2 `flow|name|target|<rel>` line has exactly four `|`-fields and is left
  * untouched (see [[LineCodec]]). Format v3 adds a *fifth* field, `once`, and
  * generalizes the fourth from a bare `Rel` render to a [[FlowDetails]] render.
  * The field count (4 vs 5) is the discriminator the parser keys on.
  *
  * Grammar of the (percent-decoded) details field — a flat, space-separated
  * token stream. Each embedded `Ex`/`Rel` sub-tree is itself percent-encoded
  * into a single atom (so it carries no spaces and never breaks tokenizing):
  * {{{
  * details := wr <enc(rel)>
  *          | autocdc <enc(source)> <scd> (keys <n> <enc(ex)>*)
  *                    (seq <enc(ex)>) (del 0|1 <enc(ex)>?) (trunc 0|1 <enc(ex)>?)
  *                    (cols <n> <enc(ex)>*) (except <n> <enc(ex)>*)
  *                    (ignidx <n> <enc(ex)>*) (ignexc <n> <enc(ex)>*)
  *                    [track <n> <enc(ex)>*] [trackexc <n> <enc(ex)>*]
  * scd     := scd1 | scd2
  * }}}
  * The leading tag (`wr` / `autocdc`) discriminates the two shapes. Round-trips:
  * `parse(render(d)) == Right(d)` for every `d`.
  *
  * The two bracketed SCD2 groups (roadmap S2) are **optional and emitted only
  * when non-empty**, and they are the LAST groups in the stream. That is what
  * keeps format v3 at v3: every AUTO CDC flow that existed before SCD2 renders
  * byte-identically (an empty track-history list is spelled by absence, exactly
  * as the wire spells it), so the manifest header, the cache hashes and the
  * fragment strings do not churn for pipelines that do not use SCD2.
  */
private[core] object FlowCodec:

  // The details field is a space-separated token stream, so atoms use the
  // empty-safe encoding (LineCodec.encAtom) — an empty `source` would otherwise
  // vanish from the stream.
  import LineCodec.{decodeAtom as decode, encAtom as enc}

  // ------------------------------------------------------------------ render

  def renderDetails(details: FlowDetails): String = details match
    case FlowDetails.WriteRelation(rel) =>
      s"wr ${encEx(RelCodec.render(rel))}"
    case cdc: FlowDetails.AutoCdc =>
      val sb = new StringBuilder
      sb.append("autocdc ").append(enc(cdc.source)).append(' ').append(scdTag(cdc.scdType))
      appendExList(sb, "keys", cdc.keys)
      sb.append(" seq ").append(encExNode(cdc.sequenceBy))
      appendOpt(sb, "del", cdc.applyAsDeletes)
      appendOpt(sb, "trunc", cdc.applyAsTruncates)
      appendExList(sb, "cols", cdc.columnList)
      appendExList(sb, "except", cdc.exceptColumnList)
      appendExList(sb, "ignidx", cdc.ignoreNullUpdatesColumnList)
      appendExList(sb, "ignexc", cdc.ignoreNullUpdatesExceptColumnList)
      // Optional trailing groups — absent when empty, so pre-SCD2 renders are
      // untouched (see the grammar note above).
      appendExListIfAny(sb, "track", cdc.trackHistoryColumnList)
      appendExListIfAny(sb, "trackexc", cdc.trackHistoryExceptColumnList)
      sb.toString

  private def appendExList(sb: StringBuilder, tag: String, exprs: List[Ex]): Unit =
    sb.append(' ').append(tag).append(' ').append(exprs.size)
    exprs.foreach(e => sb.append(' ').append(encExNode(e)))

  /** An expression group that is simply absent when empty — the spelling the
    * optional (SCD2) groups use, so that adding them cost no bytes to any
    * manifest that does not use them. */
  private def appendExListIfAny(sb: StringBuilder, tag: String, exprs: List[Ex]): Unit =
    if exprs.nonEmpty then appendExList(sb, tag, exprs)

  private def appendOpt(sb: StringBuilder, tag: String, opt: Option[Ex]): Unit =
    opt match
      case Some(e) => sb.append(' ').append(tag).append(" 1 ").append(encExNode(e))
      case None    => sb.append(' ').append(tag).append(" 0")

  /** A rendered `Ex`, percent-encoded into a single space-free atom. */
  private def encExNode(e: Ex): String = encEx(RelCodec.renderEx(e))
  private def encEx(rendered: String): String = enc(rendered)

  // ------------------------------------------------------------------ parse

  def parseDetails(text: String): Either[String, FlowDetails] =
    val tokens = text.split("\\s+").toList.filter(_.nonEmpty)
    tokens match
      case "wr" :: rel :: Nil =>
        // Total: every embedded atom is percent-decoded through
        // LineCodec.decode, so a malformed escape is a Left, not a throw.
        decode(rel).flatMap(RelCodec.parse).map(FlowDetails.WriteRelation(_))
      case "autocdc" :: source :: scd :: rest =>
        decode(source).flatMap(parseAutoCdc(_, scd, rest))
      case other =>
        Left(s"unrecognized flow details: ${other.take(3).mkString(" ")}")

  private def parseAutoCdc(
      source: String,
      scd: String,
      tokens: List[String],
  ): Either[String, FlowDetails] =
    for
      scdType <- parseScd(scd)
      r0      <- expectExList("keys", tokens)
      (keys, r1) = r0
      r2      <- expectTag("seq", r1)
      seq     <- r2.headOption.toRight("autocdc: missing sequence_by expression").flatMap(decodeEx)
      r3       = r2.drop(1)
      d0      <- expectOpt("del", r3)
      (del, r4) = d0
      t0      <- expectOpt("trunc", r4)
      (trunc, r5) = t0
      c0      <- expectExList("cols", r5)
      (cols, r6) = c0
      e0      <- expectExList("except", r6)
      (except, r7) = e0
      i0      <- expectExList("ignidx", r7)
      (ignIdx, r8) = i0
      x0      <- expectExList("ignexc", r8)
      (ignExc, r9) = x0
      h0      <- optionalExList("track", r9)
      (track, r10) = h0
      h1      <- optionalExList("trackexc", r10)
      (trackExc, r11) = h1
      _       <- if r11.isEmpty then Right(()) else Left(s"autocdc: trailing tokens: ${r11.take(3).mkString(" ")}")
    yield FlowDetails.AutoCdc(
      source = source,
      keys = keys,
      sequenceBy = seq,
      applyAsDeletes = del,
      applyAsTruncates = trunc,
      columnList = cols,
      exceptColumnList = except,
      ignoreNullUpdatesColumnList = ignIdx,
      ignoreNullUpdatesExceptColumnList = ignExc,
      scdType = scdType,
      trackHistoryColumnList = track,
      trackHistoryExceptColumnList = trackExc,
    )

  /** Consume `<tag> <n> <atom>*` and decode the n atoms as expressions. */
  private def expectExList(tag: String, tokens: List[String]): Either[String, (List[Ex], List[String])] =
    expectTag(tag, tokens).flatMap {
      case n :: rest =>
        n.toIntOption.toRight(s"autocdc: $tag count not an int: $n").flatMap { count =>
          val (taken, remaining) = rest.splitAt(count)
          if taken.sizeIs < count then Left(s"autocdc: $tag expected $count expressions")
          else traverse(taken)(decodeEx).map(_ -> remaining)
        }
      case Nil => Left(s"autocdc: $tag missing count")
    }

  /** Consume `<tag> <n> <atom>*` **if the group is there at all** — an absent
    * group is the empty list, not an error. Only the optional (trailing) SCD2
    * groups use this; every group present since v3 stays mandatory, so a
    * truncated stream is still a parse error rather than a silent default. */
  private def optionalExList(tag: String, tokens: List[String]): Either[String, (List[Ex], List[String])] =
    tokens match
      case `tag` :: _ => expectExList(tag, tokens)
      case _          => Right(Nil -> tokens)

  /** Consume `<tag> 0` (None) or `<tag> 1 <atom>` (Some). */
  private def expectOpt(tag: String, tokens: List[String]): Either[String, (Option[Ex], List[String])] =
    expectTag(tag, tokens).flatMap {
      case "0" :: rest => Right(None -> rest)
      case "1" :: atom :: rest => decodeEx(atom).map(e => Some(e) -> rest)
      case other => Left(s"autocdc: malformed optional '$tag': ${other.take(2).mkString(" ")}")
    }

  private def expectTag(tag: String, tokens: List[String]): Either[String, List[String]] =
    tokens match
      case `tag` :: rest => Right(rest)
      case other         => Left(s"autocdc: expected '$tag', got: ${other.take(1).mkString}")

  private def decodeEx(atom: String): Either[String, Ex] = decode(atom).flatMap(RelCodec.parseEx)

  private def parseScd(tag: String): Either[String, ScdType] = tag match
    case "scd1" => Right(ScdType.Scd1)
    case "scd2" => Right(ScdType.Scd2)
    case other  => Left(s"unknown scd type: $other")

  private def scdTag(scd: ScdType): String = scd match
    case ScdType.Scd1 => "scd1"
    case ScdType.Scd2 => "scd2"

  private def traverse[A](items: List[String])(f: String => Either[String, A]): Either[String, List[A]] =
    items.foldRight[Either[String, List[A]]](Right(Nil)) { (item, acc) =>
      for { a <- f(item); rest <- acc } yield a :: rest
    }
