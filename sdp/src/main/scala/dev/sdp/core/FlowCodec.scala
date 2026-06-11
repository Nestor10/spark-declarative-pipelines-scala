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
  * scd     := scd1
  * }}}
  * The leading tag (`wr` / `autocdc`) discriminates the two shapes. Round-trips:
  * `parse(render(d)) == Right(d)` for every `d`.
  */
private[core] object FlowCodec:

  import LineCodec.{enc, dec}

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
      sb.toString

  private def appendExList(sb: StringBuilder, tag: String, exprs: List[Ex]): Unit =
    sb.append(' ').append(tag).append(' ').append(exprs.size)
    exprs.foreach(e => sb.append(' ').append(encExNode(e)))

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
        RelCodec.parse(dec(rel)).map(FlowDetails.WriteRelation(_))
      case "autocdc" :: source :: scd :: rest =>
        parseAutoCdc(dec(source), scd, rest)
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
      _       <- if r9.isEmpty then Right(()) else Left(s"autocdc: trailing tokens: ${r9.take(3).mkString(" ")}")
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

  private def decodeEx(atom: String): Either[String, Ex] = RelCodec.parseEx(dec(atom))

  private def parseScd(tag: String): Either[String, ScdType] = tag match
    case "scd1" => Right(ScdType.Scd1)
    case other  => Left(s"unknown scd type: $other")

  private def scdTag(scd: ScdType): String = scd match
    case ScdType.Scd1 => "scd1"

  private def traverse[A](items: List[String])(f: String => Either[String, A]): Either[String, List[A]] =
    items.foldRight[Either[String, List[A]]](Right(Nil)) { (item, acc) =>
      for { a <- f(item); rest <- acc } yield a :: rest
    }
