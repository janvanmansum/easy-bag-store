package nl.knaw.dans.easy.bagstore

import java.nio.file.Files

import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.Try

/**
 * Represents a range in a TAR file.
 */
case class TarRange(entries: Seq[EntrySpec], startByte: Long, endByte: Option[Long] = None) extends DebugEnhancedLogging {
  type Position = Long
  type Length = Long

  private val TAR_RECORD_LENGTH = 512

  /**
   * The start positions of the entries.
   */
  private lazy val entryPositions: Seq[(EntrySpec, (Position, Length))] = {
    val entryLengths = entries.map(getTarEntryLength)
    if(logger.underlying.isDebugEnabled) debug(s"entryLengths: $entryLengths")
    val entryPositions = entryLengths.scanLeft(0L) {
      case (pos, len) => pos + len
    }
    if(logger.underlying.isDebugEnabled) debug(s"entryPositions: $entryPositions")
    entries.zip(entryPositions.zip(entryLengths))
  }

  val overlappingEntries: Seq[EntrySpec] = {
    entryPositions
      .filter {
        case (entry, (pos, len)) =>
          if (logger.underlying.isDebugEnabled) debug(s"entry = ${ entry.entryPath }, pos = $pos, len = $len")
          true
      }
      .filter { case (_, (pos, len)) => pos + len > startByte }
      .takeWhile { case (_, (pos, len)) => pos <= endByte.getOrElse(Long.MaxValue - 1) }
      .map { case (entry, (_, _)) => entry }.toList
  }

  val offsetIntoFirstEntry: Long = {
    startByte - entryPositions.takeWhile { case (entry, _) => !overlappingEntries.contains(entry)}.map {
      case (_, (_, len)) => len
    }.sum
  }

  /**
   * The total length of the range.
   */
  val totalLength: Long = endByte.getOrElse(Long.MaxValue - 1) - startByte + 1

  /**
   * Calculates the size of the entry within the TAR. An entry is always a multiple of the TAR record length and
   * always has a header (which is one record long).
   *
   * @param entrySpec the entry to calculate the length for
   * @return the length
   */
  private def getTarEntryLength(entrySpec: EntrySpec): Long = {
    val fileSize = entrySpec.sourcePath.map(Files.size).getOrElse(0L)
    val nRecords = fileSize / TAR_RECORD_LENGTH + 1 + (if (fileSize % TAR_RECORD_LENGTH == 0) 0
                                                       else 1)
    nRecords * TAR_RECORD_LENGTH
  }
}
