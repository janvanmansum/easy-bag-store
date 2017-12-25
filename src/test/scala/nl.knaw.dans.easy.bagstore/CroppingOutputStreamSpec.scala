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

import java.io.{ ByteArrayOutputStream, OutputStream }
import java.lang.IndexOutOfBoundsException

import org.scalatest.{ FlatSpec, Matchers }

class CroppingOutputStreamSpec extends FlatSpec with Matchers {

  private val alphabetBuffer: Array[Byte] = ('a' to 'z').map(_.toByte).toArray[Byte]

  "write(Byte)" should "start immediately if skipLength == 0" in {
    val output = new ByteArrayOutputStream()
    val cos = new CroppingOutputStream(output, 0, 100)
    cos.write('a')
    cos.flush()
    output.toString should be ("a")
  }

  it should "not write first byte if skipLength == 1" in {
    val output = new ByteArrayOutputStream()
    val cos = new CroppingOutputStream(output, 1, 100)
    cos.write('a')
    cos.flush()
    output.toString should be ("")
  }

  it should "not write first 5 bytes if skipLength == 10" in {
    val output = new ByteArrayOutputStream()
    val cos = new CroppingOutputStream(output, 5, 100)
    cos.write('a')
    cos.write('b')
    cos.write('c')
    cos.write('d')
    cos.write('e')
    cos.write('f')
    cos.write('g')
    cos.flush()
    output.toString should be ("fg")
  }

  it should "not write anything if totalLength == 0" in {
    val output = new ByteArrayOutputStream()
    val cos = new CroppingOutputStream(output, 0, 0)
    cos.write('a')
    cos.flush()
    output.toString shouldBe empty
  }

  it should "not write anything if totalLength == 0, with skipLength > 0" in {
    val output = new ByteArrayOutputStream()
    val cos = new CroppingOutputStream(output, 5, 0)
    cos.write('a')
    cos.write('b')
    cos.write('c')
    cos.write('d')
    cos.write('e')
    cos.write('f')
    cos.write('g')
    cos.flush()
    output.toString shouldBe empty
  }

  it should "write one byte if totalLength == 1" in {
    val output = new ByteArrayOutputStream()
    val cos = new CroppingOutputStream(output, 5, 1)
    cos.write('a')
    cos.write('b')
    cos.write('c')
    cos.write('d')
    cos.write('e')
    cos.write('f')
    cos.write('g')
    cos.flush()
    output.toString should be ("f")
  }

  "write(Array[Byte],Int,Int)" should "forward call to underlying stream if skipLength == 0 and totalLength great enough (1)" in {
    val output = new ByteArrayOutputStream()
    val cos = new CroppingOutputStream(output, 0, 100)
    cos.write(alphabetBuffer, 0, 5)
    cos.flush()
    output.toString should be ("abcde")
  }

  it should "forward call to underlying stream if skipLength == 0 and totalLength great enough (2)" in {
    val output = new ByteArrayOutputStream()
    val cos = new CroppingOutputStream(output, 0, 100)
    cos.write(alphabetBuffer, 2, 5)
    cos.flush()
    output.toString should be ("cdefg")
  }

  it should "stop after writing totalLength" in {
    val output = new ByteArrayOutputStream()
    val cos = new CroppingOutputStream(output, 0, 3)
    cos.write(alphabetBuffer, 2, 5)
    cos.flush()
    output.toString should be ("cde")
  }

  it should "skip skipLength bytes after offset and cut off end" in {
    val output = new ByteArrayOutputStream()
    val cos = new CroppingOutputStream(output, 17, 3)
    cos.write(alphabetBuffer, 3, 23) // Writes defg..z
    cos.flush()
    output.toString should be ("uvw")
  }

  it should "keep track of number of received bytes" in {
    // The same as previous test, but copying in parts
    val output = new ByteArrayOutputStream()
    val cos = new CroppingOutputStream(output, 17, 3)
    cos.write(alphabetBuffer, 3, 7)
    cos.write(alphabetBuffer, 10, 7)
    cos.write(alphabetBuffer, 17, 5)
    cos.write(alphabetBuffer, 22, 4)
    cos.flush()
    output.toString should be ("uvw")
  }

  it should "throw an exception with negative offset" in {
    val output = new ByteArrayOutputStream()
    val cos = new CroppingOutputStream(output, 17, 3)
    an [IndexOutOfBoundsException] should be thrownBy cos.write(alphabetBuffer, -1, 2)
  }

  it should "throw an exception with too large length" in {
    val output = new ByteArrayOutputStream()
    val cos = new CroppingOutputStream(output, 17, 3)
    an [IndexOutOfBoundsException] should be thrownBy cos.write(alphabetBuffer, 0, 27)
  }

  it should "throw an exception with too large length + offset" in {
    val output = new ByteArrayOutputStream()
    val cos = new CroppingOutputStream(output, 17, 3)
    an [IndexOutOfBoundsException] should be thrownBy cos.write(alphabetBuffer, 5, 22)
  }

  it should "throw an exception with negative length" in {
    val output = new ByteArrayOutputStream()
    val cos = new CroppingOutputStream(output, 17, 3)
    an [IndexOutOfBoundsException] should be thrownBy cos.write(alphabetBuffer, 0, -1)
  }
}
