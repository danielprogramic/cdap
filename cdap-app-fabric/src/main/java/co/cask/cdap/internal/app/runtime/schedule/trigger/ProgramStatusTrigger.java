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

package co.cask.cdap.internal.app.runtime.schedule.trigger;


import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProtoTrigger;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A Trigger that schedules a ProgramSchedule, when a certain status of a program has been achieved.
 */
public class ProgramStatusTrigger extends ProtoTrigger.ProgramStatusTrigger implements SatisfiableTrigger {
  private boolean satisfied;

  public ProgramStatusTrigger(ProgramId programId, Set<ProgramStatus> programStatuses) {
    super(programId, programStatuses);
  }

  @VisibleForTesting
  public ProgramStatusTrigger(ProgramId programId, ProgramStatus... programStatuses) {
    super(programId, new HashSet<>(Arrays.asList(programStatuses)));
  }

  @Override
  public boolean updateStatus(Notification notification) {
    satisfied = true;
    return satisfied;
  }

  @Override
  public boolean isSatisfied() {
    return satisfied;
  }

  @Override
  public List<String> getTriggerKeys() {
    return ImmutableList.of(programId.toString());
  }

  public static SatisfiableTrigger toSatisfiableTrigger(ProtoTrigger protoTrigger) {
    if (protoTrigger instanceof ProtoTrigger.ProgramStatusTrigger) {
      ProtoTrigger.ProgramStatusTrigger programStatusTrigger = (ProtoTrigger.ProgramStatusTrigger) protoTrigger;
      return new co.cask.cdap.internal.app.runtime.schedule.trigger.ProgramStatusTrigger(
        programStatusTrigger.getProgramId(), programStatusTrigger.getProgramStatuses());
    }
    throw new IllegalArgumentException(String.format("Trigger has type '%s' instead of type '%s",
                                                     protoTrigger.getType().name(), Type.PROGRAM_STATUS.name()));
  }
}
