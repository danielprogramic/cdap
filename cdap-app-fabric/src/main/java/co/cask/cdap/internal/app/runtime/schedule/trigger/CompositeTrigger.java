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

import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.internal.schedule.trigger.ScheduleTrigger;
import co.cask.cdap.internal.schedule.trigger.Trigger;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.ProtoTrigger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Abstract base class for composite trigger.
 */
public abstract class CompositeTrigger extends ProtoTrigger.AbstractCompositeTrigger
  implements SatisfiableTrigger {
  protected boolean satisfied;
  // A map of non-composite trigger type and set of triggers of the same type
  private Map<Type, Set<Trigger>> unitTriggers;

  public CompositeTrigger(Type type, Trigger... triggers) {
    super(type, triggers);
    initializeUnitTriggers();
  }

  public void initializeUnitTriggers() {
    unitTriggers = new HashMap<>();
    for (Trigger trigger : triggers) {
      // Add current non-composite trigger to the corresponding set in the map
      Type triggerType = ((ProtoTrigger) trigger).getType();
      if (!triggerType.isComposite()) {
        Set<Trigger> triggerList = unitTriggers.get(triggerType);
        if (triggerList == null) {
          triggerList = new HashSet<>();
          unitTriggers.put(triggerType, triggerList);
        }
        triggerList.add(trigger);
      } else {
        // If the current trigger is a composite trigger, add all of its unit triggers to
        for (Map.Entry<Type, Set<Trigger>> entry :
          ((CompositeTrigger) trigger).getUnitTriggers().entrySet()) {
          Set<Trigger> triggerList = unitTriggers.get(entry.getKey());
          if (triggerList == null) {
            unitTriggers.put(entry.getKey(), entry.getValue());
            continue;
          }
          triggerList.addAll(entry.getValue());
        }
      }
    }
  }

  /**
   * Only update the trigger's status with the notification if the trigger is not satisfied.
   * Skip updating if the trigger is not composite trigger and the notification type is different from the trigger type.
   */
  void doUpdateStatus(SatisfiableTrigger trigger, Notification notification) {
    if (!trigger.isSatisfied()) {
      ProtoTrigger.Type type = ((ProtoTrigger) trigger).getType();
      if (!type.isComposite() && !type.name().equals(notification.getNotificationType().name())) {
        return;
      }
      trigger.updateStatus(notification);
    }
  }

  @Override
  public boolean isSatisfied() {
    return satisfied;
  }

  @Override
  public List<String> getTriggerKeys() {
    // Only keep unique trigger keys in the set
    Set<String> triggerKeys = new HashSet<>();
    for (Trigger trigger : triggers) {
      triggerKeys.addAll(((SatisfiableTrigger) trigger).getTriggerKeys());
    }
    return new ArrayList<>(triggerKeys);
  }

  /**
   * Get all triggers which are not composite trigger in this trigger.
   */
  public Map<Type, Set<Trigger>> getUnitTriggers() {
    return unitTriggers;
  }

  public static Trigger fromScheduleTrigger(Trigger trigger, Type type) {
    if (trigger instanceof ScheduleTrigger.AbstractCompositeTrigger) {
      ScheduleTrigger.AbstractCompositeTrigger compositeTrigger = (ScheduleTrigger.AbstractCompositeTrigger) trigger;
      Trigger[] internalTriggers = compositeTrigger.getTriggers();
      Trigger[] satisfiableTriggers = new Trigger[internalTriggers.length];
      for (int i = 0; i < internalTriggers.length; i++) {
        satisfiableTriggers[i] = Schedulers.toSatisfiableTrigger((ScheduleTrigger) internalTriggers[i]);
      }
      if (type.equals(Type.AND)) {
        return new co.cask.cdap.internal.app.runtime.schedule.trigger.AndTrigger(satisfiableTriggers);
      }
      return new co.cask.cdap.internal.app.runtime.schedule.trigger.OrTrigger(satisfiableTriggers);
    }
    throw new IllegalArgumentException(String.format("Trigger of class '%s' is not an instance of '%s",
                                                     trigger.getClass().getName(),
                                                     ScheduleTrigger.AbstractCompositeTrigger.class.getName()));
  }
}
