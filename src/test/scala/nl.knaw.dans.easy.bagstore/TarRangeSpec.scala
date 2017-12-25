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

import java.nio.file.{ Files, Paths }

import org.apache.commons.io.FileUtils
import org.scalatest.{ FlatSpec, Matchers }
import resource._

import scala.collection.JavaConverters._

class TarRangeSpec extends FlatSpec with Matchers {
  private val testDataDir = Paths.get(s"target/test/${ getClass.getSimpleName }")
  FileUtils.copyDirectory(Paths.get("src/test/resources/ranges").toFile, testDataDir.toFile)

  private def testSet(name: String): Seq[EntrySpec] = managed(Files.walk(testDataDir.resolve(name))).acquireAndGet {
    paths =>
      paths.iterator().asScala.map {
        p =>
          if (Files.isRegularFile(p)) EntrySpec(Some(p), p.toString)
          else EntrySpec(None, p.toString)
      }.toList.sortBy(_.entryPath)
  }

  "overlappingEntries" should "be empty for an empty list" in {
    TarRange(Seq.empty[EntrySpec], 0, Some(0)).overlappingEntries shouldBe Seq.empty[EntrySpec]
    TarRange(Seq.empty[EntrySpec], 1, Some(1)).overlappingEntries shouldBe Seq.empty[EntrySpec]
    TarRange(Seq.empty[EntrySpec], 1, Some(5)).overlappingEntries shouldBe Seq.empty[EntrySpec]
    TarRange(Seq.empty[EntrySpec], 100, Some(3)).overlappingEntries shouldBe Seq.empty[EntrySpec]
  }

  /*
   * The test set contains files and directories for 5 entries, adding up to 9 TAR records. Below
   * a schematic representation of the resulting TAR. In parentheses the number of records for the
   * entry. Under the pipes the start position of the entry.
   *
   * | basedir (1) | adir (1) | asubfile (3) | bsubfile (2) | afile (2) |
   * 0             512        1024           2560           3584
   *             511<--------------------------------------->3584
   *
   * When finding the overlap for range 511-3584 (first test below), consider that ranges are
   * inclusive, so the entry containing byte number 511 is the start entry and the one containing
   * byte number 3584 is the last entry. This turns out to result in all the entries.
   */


  it should "be the complete list if range starts at zero and extends beyond end of data" in {
    val seq = testSet("testsetA/basedir_0_1")
    TarRange(seq, 0, Some(1000000)).overlappingEntries shouldBe seq
  }

  it should "be the complete list if range is exactly the complete TAR" in {
    val seq = testSet("testsetA/basedir_0_1")
    TarRange(seq, 0, Some(512 * 9)).overlappingEntries shouldBe seq
  }

  it should "be the complete list if range overlaps with one byte in first and last entry" in {
    val seq = testSet("testsetA/basedir_0_1")
    TarRange(seq, 511, Some(512 * 7)).overlappingEntries shouldBe seq
  }

  it should "be minus first and last entry if range exactly over entries between those" in {
    val seq = testSet("testsetA/basedir_0_1")
    TarRange(seq, 512, Some(512 * 7 - 1)).overlappingEntries shouldBe seq.slice(1, 4)
  }

  "offsetIntoFirstEntry" should "be zero if range starts with byte 0" in {
    val seq = testSet("testsetA/basedir_0_1")
    TarRange(seq, 0, Some(1000000)).offsetIntoFirstEntry should be(0)
  }

  it should "be startByte if range starts in first entry" in {
    val seq = testSet("testsetA/basedir_0_1")
    TarRange(seq, 100, Some(1000000)).offsetIntoFirstEntry should be(100)
  }

  it should "be 0 if range starts exactly on second entry" in {
    val seq = testSet("testsetA/basedir_0_1")
    TarRange(seq, 512, Some(1000000)).offsetIntoFirstEntry should be(0)
  }

  it should "be startposition withing second entry" in {
    val seq = testSet("testsetA/basedir_0_1")
    TarRange(seq, 512 + 10, Some(1000000)).offsetIntoFirstEntry should be(10)
  }

  "totalLength" should "be difference between end and start + 1" in {
    TarRange(Seq.empty[EntrySpec], 1000, Some(2000)).totalLength should be (1001)
  }

  it should "be one if start and end are the same" in {
    TarRange(Seq.empty[EntrySpec], 1000, Some(1000)).totalLength should be (1)
  }

}
