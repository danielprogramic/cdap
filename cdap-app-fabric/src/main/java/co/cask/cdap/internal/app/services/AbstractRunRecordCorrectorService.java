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

import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A default implementation of {@link RunRecordCorrectorService}.
 */
public class AbstractRunRecordCorrectorService extends AbstractIdleService implements RunRecordCorrectorService {
  private static final Logger LOG = LoggerFactory.getLogger(ProgramLifecycleService.class);

  private final Store store;
  private final ProgramLifecycleService programLifecycleService;
  private final ProgramRuntimeService runtimeService;

  @Inject
  public AbstractRunRecordCorrectorService(Store store,
                                           ProgramLifecycleService programLifecycleService,
                                           ProgramRuntimeService runtimeService) {
    this.store = store;
    this.programLifecycleService = programLifecycleService;
    this.runtimeService = runtimeService;
  }

  @Override
  public void validateAndCorrectRunningRunRecords() {
    Set<String> processedInvalidRunRecordIds = Sets.newHashSet();

    // Lets update the running programs run records
    for (ProgramType programType : ProgramType.values()) {
      validateAndCorrectRunningRunRecords(programType, processedInvalidRunRecordIds);
    }

    if (!processedInvalidRunRecordIds.isEmpty()) {
      LOG.info("Corrected {} of run records with RUNNING status but no actual program running.",
               processedInvalidRunRecordIds.size());
    }
  }

  public void validateAndCorrectRunningRunRecords(final ProgramType programType,
                                                  final Set<String> processedInvalidRunRecordIds) {
    final Map<RunId, ProgramRuntimeService.RuntimeInfo> runIdToRuntimeInfo = runtimeService.list(programType);

    LOG.trace("Start getting run records not actually running ...");
    Collection<RunRecordMeta> notActuallyRunning =
      store.getRuns(ProgramRunStatus.RUNNING,
                    new com.google.common.base.Predicate<RunRecordMeta>() {
                      @Override
                      public boolean apply(RunRecordMeta input) {
                        String runId = input.getPid();
                        // Check if it is not actually running.
                        return !runIdToRuntimeInfo.containsKey(RunIds.fromString(runId));
                      }
                    }).values();
    LOG.trace("End getting {} run records not actually running.", notActuallyRunning.size());

    final Map<String, ProgramId> runIdToProgramId = new HashMap<>();

    LOG.trace("Start getting invalid run records  ...");
    Collection<RunRecordMeta> invalidRunRecords =
      Collections2.filter(notActuallyRunning, new com.google.common.base.Predicate<RunRecordMeta>() {
        @Override
        public boolean apply(RunRecordMeta input) {
          String runId = input.getPid();
          // check for program Id for the run record, if null then it is invalid program type.
          ProgramId targetProgramId = programLifecycleService.retrieveProgramIdForRunRecord(programType, runId);

          // Check if run id is for the right program type
          if (targetProgramId != null) {
            runIdToProgramId.put(runId, targetProgramId);
            return true;
          } else {
            return false;
          }
        }
      });

    // don't correct run records for programs running inside a workflow
    // for instance, a MapReduce running in a Workflow will not be contained in the runtime info in this class
    invalidRunRecords = Collections2.filter(invalidRunRecords, new com.google.common.base.Predicate<RunRecordMeta>() {
      @Override
      public boolean apply(RunRecordMeta invalidRunRecordMeta) {
        boolean shouldCorrect = programLifecycleService.shouldCorrectForWorkflowChildren(invalidRunRecordMeta,
                                                                                         processedInvalidRunRecordIds);
        if (!shouldCorrect) {
          LOG.trace("Will not correct invalid run record {} since it's parent workflow still running.",
                    invalidRunRecordMeta);
          return false;
        }
        return true;
      }
    });

    LOG.trace("End getting invalid run records.");

    if (!invalidRunRecords.isEmpty()) {
      LOG.warn("Found {} RunRecords with RUNNING status and the program not actually running for program type {}",
               invalidRunRecords.size(), programType.getPrettyName());
    } else {
      LOG.trace("No RunRecords found with RUNNING status and the program not actually running for program type {}",
                programType.getPrettyName());
    }

    // Now lets correct the invalid RunRecords
    for (RunRecordMeta invalidRunRecordMeta : invalidRunRecords) {
      String runId = invalidRunRecordMeta.getPid();
      ProgramId targetProgramId = runIdToProgramId.get(runId);

      boolean updated = store.compareAndSetStatus(targetProgramId, runId, ProgramController.State.ALIVE.getRunStatus(),
                                                  ProgramController.State.ERROR.getRunStatus());
      if (updated) {
        LOG.warn("Fixed RunRecord {} for program {} with RUNNING status because the program was not " +
                   "actually running",
                 runId, targetProgramId);

        processedInvalidRunRecordIds.add(runId);
      }
    }
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting RunRecordCorrectorService");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping RunRecordCorrectorService");
  }

  /**
   * Helper class to run in separate thread to validate the invalid running run records
   */
  class RunRecordsCorrectorRunnable implements Runnable {

    @Override
    public void run() {
      try {
        LOG.trace("Start correcting invalid run records ...");

        // Lets update the running programs run records
        validateAndCorrectRunningRunRecords();

        LOG.trace("End correcting invalid run records.");
      } catch (Throwable t) {
        // Ignore any exception thrown since this behaves like daemon thread.
        //noinspection ThrowableResultOfMethodCallIgnored
        LOG.warn("Unable to complete correcting run records: {}", Throwables.getRootCause(t).getMessage());
        LOG.debug("Exception thrown when running run id cleaner.", t);
      }
    }
  }
}
