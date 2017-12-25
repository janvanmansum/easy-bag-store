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

import nl.knaw.dans.lib.logging.DebugEnhancedLogging

/**
 * Wraps another output stream and crops its input. "Cropping" here means cutting off parts of the beginning and/or
 * the end, similar to cropping an image.
 *
 * @param os the wrapped output stream
 * @param skipLength the number of bytes to cut off the beginning of data
 * @param maxLength maximum length to write before cutting off the end
 */
class CroppingOutputStream(os: => OutputStream, skipLength: Long, maxLength: Long) extends OutputStream with DebugEnhancedLogging {
  private var received = 0L
  private var written = 0L

  override def write(b: Int): Unit = {
    if (received >= skipLength && written < maxLength) {
      os.write(b)
      written += 1
    }
    received += 1
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    trace(b.length, off, len)
    // Copied the checks on bounds from java.io.OutputStream
    if (off < 0 || off > b.length || len < 0 || off + len > b.length || off + len < 0) throw new IndexOutOfBoundsException
    val startPosInBufferPart = Math.max(skipLength - received, 0)
    val startPosInBuffer = off + startPosInBufferPart
    val nBytesToWrite = Math.min(len, Math.min(maxLength - written, b.length - startPosInBuffer))
    if (startPosInBuffer < b.length) {
      debug(s"Writing from input to underlying buffer: start = $startPosInBuffer, len = $nBytesToWrite")
      try {
        os.write(b, startPosInBuffer.toInt, nBytesToWrite.toInt)
      } catch {
        case e: Exception => debug(s"Exception: $e"); throw e
      }
      written += nBytesToWrite
    }
    received += len
    debug(s"received = $received, written = $written")
  }

  override def close(): Unit = os.close()

  override def flush(): Unit = os.flush()
}
