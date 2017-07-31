/*
 * Copyright © 2017 Cask Data, Inc.
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


package co.cask.cdap.internal.app.services;

import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.proto.ProgramType;
import com.google.common.util.concurrent.Service;

import java.util.Set;

/**
 * A service interface that defines the behavior when run records are corrected
 */
public interface RunRecordCorrectorService extends Service {
  /**
   * @see #validateAndCorrectRunningRunRecords(ProgramType, Set)
   */
  void validateAndCorrectRunningRunRecords();

  /**
   * Fix all the possible inconsistent states for RunRecords that shows it is in RUNNING state but actually not
   * via check to {@link ProgramRuntimeService} for a type of CDAP program.
   *
   * @param programType The type of program the run records need to validate and update.
   * @param processedInvalidRunRecordIds the {@link Set} of processed invalid run record ids.
   */
  void validateAndCorrectRunningRunRecords(final ProgramType programType,
                                           final Set<String> processedInvalidRunRecordIds);
}
