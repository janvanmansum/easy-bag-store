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
package nl.knaw.dans.easy.bagstore.server

import org.scalatest.{ FlatSpec, Matchers }

class TranslateRangeHeaderSpec extends FlatSpec with Matchers {

  "translateRangeHeader" should "return start and end bytes for properly formatted single range" in {
    translateRangeHeader("bytes=1-10") should be (1, Some(10))
  }

  it should "return default value for anything other than bytes" in {
    translateRangeHeader("files=1-10") should be (0, None)
  }

  it should "return default value for range off the end" in {
    translateRangeHeader("bytes=-10") should be (0, None)
  }

  it should "return open ending range if no end byte specified" in {
    translateRangeHeader("bytes=10-") should be (10, None)
  }

  it should "return default value if passed null" in {
    translateRangeHeader(null) should be (0, None)
  }

  it should "return default value if passed garbage" in {
    translateRangeHeader("garbage :)") should be (0, None)
  }

}
