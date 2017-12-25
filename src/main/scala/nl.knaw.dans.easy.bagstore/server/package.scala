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

import scala.util.Try

package object server {
  type StartByte = Long
  type EndByte = Long
  private val defaultRange = (0L, None)

  /**
   * Translates the value of the Range header into the range that should be returned to the client.
   * No Range header means that the range "bytes=0-" must be returned.
   *
   * We only support simple ranges of bytes, that do not require us to know the content length in
   * advance; so, *not* supported are:
   *
   * - multiple ranges (e.g., bytes=1-10,20-30)
   * - ranges of other units (actually, there seem to be no other units defined yet)
   * - ranges off the end of the content (e.g., bytes=-500 for the last 500 bytes)
   *
   * In RFC7233 there seem to be no status codes for signalling the lack of support for these features, so we
   * take the simplistic approach of converting anything we don't understand into a request for the
   * complete content.
   *
   * @see http://svn.tools.ietf.org/svn/wg/httpbis/specs/rfc7233.html
   *
   * @param headerValue the string containing the range specification
   * @return the start and optional end byte numbers
   */

  def translateRangeHeader(headerValue: String): (StartByte, Option[EndByte]) = {
    Option(headerValue).map {
      _.split("=", 2) match {
        case Array("bytes", rangeString) => rangeString.split("-", 2) match {
          case Array(start, end) if start.nonEmpty => (start.toLong, if (end.isEmpty) None
                                                                     else Some(end.toLong))
          case _ => defaultRange
        }
        case _ => defaultRange
      }
    }.getOrElse(defaultRange)
  }
}
