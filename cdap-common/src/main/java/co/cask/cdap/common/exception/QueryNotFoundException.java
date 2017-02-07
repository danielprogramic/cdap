/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.common.exception;

import co.cask.cdap.proto.Id;

/**
 * Thrown when a query was not found by its handle.
 * @deprecated Use {@link co.cask.cdap.common.QueryNotFoundException} instead
 */
@Deprecated
public class QueryNotFoundException extends NotFoundException {

  private final Id.QueryHandle id;

  // TODO: namespace?
  public QueryNotFoundException(Id.QueryHandle id) {
    super(id);
    this.id = id;
  }

  public Id.QueryHandle getId() {
    return id;
  }
}