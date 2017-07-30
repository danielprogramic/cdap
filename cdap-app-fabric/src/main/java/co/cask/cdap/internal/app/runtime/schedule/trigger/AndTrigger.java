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

import co.cask.cdap.proto.Notification;

/**
 * A Trigger that schedules a ProgramSchedule, when all internal triggers are satisfied.
 */
public class AndTrigger extends CompositeTrigger implements SatisfiableTrigger {

  public AndTrigger(SatisfiableTrigger... triggers) {
    super(Type.AND, triggers);
  }

  @Override
  public boolean updateStatus(Notification notification) {
    if (satisfied) {
      return true;
    }
    boolean satisfied = true;
    for (SatisfiableTrigger trigger : triggers) {
      doUpdateStatus(trigger, notification);
      if (!trigger.isSatisfied() && satisfied) {
        satisfied = false;
      }
    }
    return this.satisfied = satisfied;
  }
}
