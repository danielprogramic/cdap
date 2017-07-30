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
  protected final SatisfiableTrigger[] triggers;
  protected boolean isSatisfied;
  // A map of non-composite trigger type and set of triggers of the same type
  private final Map<Type, Set<SatisfiableTrigger>> unitTriggers;

  public CompositeTrigger(Type type, SatisfiableTrigger... triggers) {
    super(type, triggers);
    this.triggers = triggers;
    unitTriggers = new HashMap<>();
    initializeUnitTriggers();
  }

  // TODO: call this method in TriggerCodec
  private void initializeUnitTriggers() {
    for (SatisfiableTrigger trigger : triggers) {
      // Add current non-composite trigger to the corresponding set in the map
      Type triggerType = ((ProtoTrigger) trigger).getType();
      if (!triggerType.isComposite()) {
        Set<SatisfiableTrigger> triggerList = unitTriggers.get(triggerType);
        if (triggerList == null) {
          triggerList = new HashSet<>();
          unitTriggers.put(triggerType, triggerList);
        }
        triggerList.add(trigger);
      } else {
        // If the current trigger is a composite trigger, add all of its unit triggers to
        for (Map.Entry<Type, Set<SatisfiableTrigger>> entry :
          ((CompositeTrigger) trigger).getUnitTriggers().entrySet()) {
          Set<SatisfiableTrigger> triggerList = unitTriggers.get(entry.getKey());
          if (triggerList == null) {
            unitTriggers.put(entry.getKey(), entry.getValue());
            continue;
          }
          triggerList.addAll(entry.getValue());
        }
      }
    }
  }

  @Override
  public List<String> getTriggerKeys() {
    // Only keep unique trigger keys in the set
    Set<String> triggerKeys = new HashSet<>();
    for (SatisfiableTrigger trigger : triggers) {
      triggerKeys.addAll(trigger.getTriggerKeys());
    }
    return new ArrayList<>(triggerKeys);
  }

  /**
   * Get all triggers which are not composite trigger in this trigger.
   */
  public Map<Type, Set<SatisfiableTrigger>> getUnitTriggers() {
    return unitTriggers;
  }
}
