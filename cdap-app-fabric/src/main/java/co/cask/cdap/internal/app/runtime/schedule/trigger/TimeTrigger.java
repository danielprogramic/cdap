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

import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.internal.schedule.trigger.ScheduleTrigger;
import co.cask.cdap.internal.schedule.trigger.Trigger;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProtoTrigger;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.util.List;
import java.util.Map;

/**
 * A Trigger that schedules a ProgramSchedule, based upon a particular cron expression.
 */
public class TimeTrigger extends ProtoTrigger.TimeTrigger implements Trigger, SatisfiableTrigger {
  private static final Gson GSON = new Gson();
  private static final java.lang.reflect.Type STRING_STRING_MAP = new TypeToken<Map<String, String>>() { }.getType();

  private boolean satisfied;

  public TimeTrigger(String cronExpression) {
    super(cronExpression);
    validate();
  }

  @Override
  public void validate() {
    Schedulers.validateCronExpression(cronExpression);
  }

  @Override
  public boolean updateStatus(Notification notification) {
    if (satisfied) {
      return true;
    }
    String systemOverridesString = notification.getProperties().get(ProgramOptionConstants.SYSTEM_OVERRIDES);
    if (systemOverridesString != null) {
      Map<String, String> systemOverrides = GSON.fromJson(systemOverridesString, STRING_STRING_MAP);
      return satisfied = cronExpression.equals(systemOverrides.get(ProgramOptionConstants.CRON_EXPRESSION));
    }
    return false;
  }

  @Override
  public boolean isSatisfied() {
    return satisfied;
  }

  @Override
  public List<String> getTriggerKeys() {
    return ImmutableList.of();
  }

  public static Trigger from(Trigger trigger) {
    if (trigger instanceof ScheduleTrigger.TimeTrigger) {
      return new co.cask.cdap.internal.app.runtime.schedule.trigger.TimeTrigger(
        ((ScheduleTrigger.TimeTrigger) trigger).getCronExpression());
    }
    throw new IllegalArgumentException(String.format("Trigger of class '%s' is not an instance of '%s",
                                                     trigger.getClass().getName(),
                                                     ScheduleTrigger.TimeTrigger.class.getName()));
  }
}
