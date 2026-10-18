package newspipe.infrastructure.env

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.annotation.tailrec
import scala.collection.mutable

final case class DotEnv(values: Map[String, String], source: Option[Path]):
  def get(key: String): Option[String] = values.get(key)
  def keys: Set[String] = values.keySet

object DotEnv:
  private val MaxDepth = 10

  def empty: DotEnv = DotEnv(Map.empty, None)

  def discover(start: Path): DotEnv =
    @tailrec def walk(dir: Path, acc: Option[Path]): DotEnv =
      if dir == null then acc match
        case Some(candidate) =>
          val text: String = Files.readString(candidate, StandardCharsets.UTF_8)
          DotEnv(expand(parse(text)).fold(m => Map.empty, identity), Some(candidate))
        case None => DotEnv.empty
      else
        val candidate: Path = dir.resolve(".env")
        if Files.isRegularFile(candidate) then
          walk(dir.getParent, Some(candidate))
        else walk(dir.getParent, acc)
    walk(start.toAbsolutePath.normalize(), None)

  private def parse(text: String): Map[String, String] =
    val acc: mutable.Map[String, String] = mutable.Map.empty
    text.linesIterator.foreach: line =>
      val trimmed: String = line.trim
      if trimmed.nonEmpty && !trimmed.startsWith("#") then
        val idx: Int = trimmed.indexOf('=')
        if idx > 0 then
          acc.update(trimmed.substring(0, idx).trim, unquote(trimmed.substring(idx + 1).trim))
    acc.toMap

  private def unquote(raw: String): String =
    val quoted: Boolean = isQuoted(raw)
    if quoted then raw.substring(1, raw.length - 1) else raw

  private def isQuoted(raw: String): Boolean =
    raw.length >= 2 &&
      ((raw.startsWith("'") && raw.endsWith("'")) || (raw.startsWith("\"") && raw.endsWith("\"")))

  def expand(raw: Map[String, String]): Either[String, Map[String, String]] =
    val out: mutable.Map[String, String] = mutable.Map.empty
    val keys: List[String] = raw.keys.toList.sorted
    keys.foreach: key =>
      resolve(key, raw, 0, Nil) match
        case Right(v) => out.update(key, v)
        case Left(e) => return Left(e)
    Right(out.toMap)

  private def resolve(key: String, raw: Map[String, String], depth: Int, seen: List[String]): Either[String, String] =
    if depth > MaxDepth then Left(s"expansion depth exceeded resolving '$key'")
    else if seen.contains(key) then Left(s"circular reference: ${(seen :+ key).mkString(" -> ")}")
    else
      raw.get(key) match
        case None => Right("")
        case Some(v) =>
          if isQuoted(v) then Right(unquote(v))
          else if !v.contains("${") && raw.contains(v) then
            resolve(v, raw, depth + 1, seen :+ key)
          else substitute(v, raw, depth + 1, seen :+ key)

  private def substitute(text: String, raw: Map[String, String], depth: Int, seen: List[String]): Either[String, String] =
    val out: StringBuilder = StringBuilder.newBuilder
    var idx: Int = 0
    while idx < text.length do
      text.charAt(idx) match
        case '$' if idx + 1 < text.length && text.charAt(idx + 1) == '{' =>
          val close: Int = text.indexOf('}', idx + 2)
          if close < 0 then
            out.append(text.substring(idx))
            idx = text.length
          else
            resolve(text.substring(idx + 2, close), raw, depth, seen) match
              case Right(v) => out.append(v)
              case Left(e) => return Left(e)
            idx = close + 1
        case c =>
          out.append(c)
          idx = idx + 1
    Right(out.result())
