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

class CroppingOutputStream(os: OutputStream, skipLength: Long, totalLengthToWrite: Long) extends OutputStream {
  private var received = 0L
  private var written = 0L

  override def write(b: Int): Unit = {
    if (skipLength < received) {
      os.write(b)
      written += 1
    }
    received += 1
  }

  override def write(b: Array[Byte]): Unit = {
    write(b, 0, b.length)
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    val startPosInBufferPart = Math.min(Math.max(skipLength - received, 0), b.length - len - 1)
    val nBytesToWrite = Math.min(totalLengthToWrite - written, len - startPosInBufferPart)
    os.write(b, off + startPosInBufferPart.toInt, nBytesToWrite.toInt)
    received += b.length
    written += nBytesToWrite
  }

  override def close(): Unit = os.close()

  override def flush(): Unit = os.flush()
}
