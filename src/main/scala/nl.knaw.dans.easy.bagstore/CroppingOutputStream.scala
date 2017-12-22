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
}
