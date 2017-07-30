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
 * A Trigger that schedules a ProgramSchedule, when at least one of the internal triggers are satisfied.
 */
public class OrTrigger extends CompositeTrigger implements SatisfiableTrigger {

  public OrTrigger(SatisfiableTrigger... triggers) {
    super(Type.OR, triggers);
  }

  @Override
  public boolean updateStatus(Notification notification) {
    if (isSatisfied) {
      return true;
    }
    for (SatisfiableTrigger trigger : triggers) {
      // Call isSatisfied on all triggers to update their status with the new notification
      if (trigger.updateStatus(notification)) {
        return isSatisfied = true;
      }
    }
    return false;
  }
}
