/**
 * Copyright (C) 2016 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.bagstore

import java.io.OutputStream
import java.nio.file.{ Files, Path }

import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream
import org.apache.commons.compress.archivers.{ ArchiveOutputStream, ArchiveStreamFactory }
import resource.ManagedResource

import scala.language.implicitConversions
import scala.util.{ Success, Try }

object ArchiveStreamType extends Enumeration {
  type ArchiveStreamType = Value
  val TAR, ZIP = Value
}

import nl.knaw.dans.easy.bagstore.ArchiveStreamType._

/**
 * Specification for an entry in the archive file.
 *
 * @param sourcePath optional path to an existing file or directory, if None a directory entry will be created
 * @param entryPath  the path of the entry in the archive file
 */
case class EntrySpec(sourcePath: Option[Path], entryPath: String)

/**
 * Object representing a TAR ball, providing a function to write it to an output stream.
 *
 * @param files the files in the TAR ball
 */
class ArchiveStream(streamType: ArchiveStreamType, files: Seq[EntrySpec]) extends DebugEnhancedLogging {

  implicit private def toArchiveStreamFactory(streamType: ArchiveStreamType.Value): String = {
    streamType match {
      case TAR => ArchiveStreamFactory.TAR
      case ZIP => ArchiveStreamFactory.ZIP
    }
  }

  /**
   * Writes the files to an output stream.
   *
   * @param outputStream the output stream to write to
   * @return
   */
  def writeTo(outputStream: => OutputStream): Try[Unit] = {
    createArchiveOutputStream(outputStream).map(_.acquireAndGet { aos =>
      setLongFileNameSupport(aos)
      files.toStream.map(addFileToArchiveOutputStream(aos))
        .map(r => { debug(s"Result is: $r"); r })
        .find(_.isFailure).getOrElse(finishArchiveOutputStream(aos))
    })
  }

  private def finishArchiveOutputStream(aos: ArchiveOutputStream): Try[Unit] = Try {
    aos.finish()
  }

  private def setLongFileNameSupport(aos: ArchiveOutputStream): Unit = {
    aos match {
      case tar: TarArchiveOutputStream => tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_TRUNCATE)
    }
  }


  private def createArchiveOutputStream(output: => OutputStream): Try[ManagedResource[ArchiveOutputStream]] = Try {
    resource.managed(new ArchiveStreamFactory("UTF-8").createArchiveOutputStream(streamType, output))
  }

  private def addFileToArchiveOutputStream(os: ArchiveOutputStream)(entrySpec: EntrySpec): Try[Unit] = Try {
    debug(s"Adding entry: ${ entrySpec.entryPath }...")
    val entry = os.createArchiveEntry(entrySpec.sourcePath.map(_.toFile).orNull, entrySpec.entryPath)
    debug(s"Putting entry: ${ entrySpec.entryPath }...")
    os.putArchiveEntry(entry)
    debug(s"Copying file data: ${ entrySpec.entryPath }...")
    entrySpec.sourcePath.foreach { case file if Files.isRegularFile(file) => Files.copy(file, os) }
    debug(s"Closing entry: ${ entrySpec.entryPath }...")
    os.closeArchiveEntry()
    debug(s"Done adding entry: ${ entrySpec.entryPath }")
  }
}
