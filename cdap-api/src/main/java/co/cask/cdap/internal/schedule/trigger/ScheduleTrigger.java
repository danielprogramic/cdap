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

package co.cask.cdap.internal.schedule.trigger;

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

import co.cask.cdap.api.ProgramStatus;
import co.cask.cdap.api.app.ProgramType;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;



/**
 * Represents a trigger in a schedule to be added in an app.
 */
public abstract class ScheduleTrigger implements Trigger {

  /**
   * Represents all known trigger types in an app.
   */
  public enum Type {
    TIME,
    STREAM_SIZE,
    PARTITION,
    PROGRAM_STATUS,
    AND(true),
    OR(true);

    private final boolean isComposite;

    Type() {
      this.isComposite = false;
    }

    Type(boolean isComposite) {
      this.isComposite = isComposite;
    }

    /**
     * Whether the type trigger is a composite, i.e. a trigger that can contains multiple triggers internally.
     */
    public boolean isComposite() {
      return isComposite;
    }
  }

  private final Type type;

  private ScheduleTrigger(Type type) {
    this.type = type;
  }

  public Type getType() {
    return type;
  }

  public abstract void validate();

  /**
   * Represents a time trigger in an app.
   */
  public static class TimeTrigger extends ScheduleTrigger {

    protected final String cronExpression;

    public TimeTrigger(String cronExpression) {
      super(Type.TIME);
      this.cronExpression = cronExpression;
      validate();
    }

    public String getCronExpression() {
      return cronExpression;
    }

    @Override
    public boolean equals(Object o) {
      return this == o ||
        o != null
          && getClass().equals(o.getClass())
          && Objects.equals(getCronExpression(), ((TimeTrigger) o).getCronExpression());
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(getCronExpression());
    }

    @Override
    public String toString() {
      return "TimeTrigger(" + getCronExpression() + "\")";
    }

    @Override
    public void validate() {
      validateNotNull(getCronExpression(), "cron expression");
    }
  }

  /**
   * Represents a partition trigger in an app.
   */
  public static class PartitionTrigger extends ScheduleTrigger {

    private final String datasetNamespace;
    private final String datasetName;
    protected final int numPartitions;

    public PartitionTrigger(String datasetNamespace, String datasetName, int numPartitions) {
      super(Type.PARTITION);
      this.datasetNamespace = datasetNamespace;
      this.datasetName = datasetName;
      this.numPartitions = numPartitions;
      validate();
    }

    public String getDatasetNamespace() {
      return datasetNamespace;
    }

    public String getDatasetName() {
      return datasetName;
    }

    public int getNumPartitions() {
      return numPartitions;
    }

    @Override
    public void validate() {
      validateNotNull(getDatasetNamespace(), "dataset namespace");
      validateNotNull(getDatasetName(), "dataset name");
      validateInRange(getNumPartitions(), "number of partitions", 1, null);
    }

    @Override
    public boolean equals(Object o) {
      return this == o ||
        o != null &&
          getClass().equals(o.getClass()) &&
          Objects.equals(getDatasetNamespace(), ((PartitionTrigger) o).getDatasetNamespace()) &&
          Objects.equals(getDatasetName(), ((PartitionTrigger) o).getDatasetName()) &&
          Objects.equals(getNumPartitions(), ((PartitionTrigger) o).getNumPartitions());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getDatasetNamespace(), getDatasetName(), getNumPartitions());
    }

    @Override
    public String toString() {
      return String.format("PartitionTrigger(%s.%s, %d partitions)", 
                           getDatasetNamespace(), getDatasetName(), getNumPartitions());
    }
  }

  /**
   * A Trigger builder that builds a {@link PartitionTriggerBuilder}.
   */
  public static class PartitionTriggerBuilder extends ScheduleTrigger implements TriggerBuilder {
    private final String datasetName;
    private final int numPartitions;

    public PartitionTriggerBuilder(String datasetName, int numPartitions) {
      super(Type.PARTITION);
      this.datasetName = datasetName;
      this.numPartitions = numPartitions;
      validate();
    }

    @Override
    public void validate() {
      validateNotNull(datasetName, "dataset name");
      validateInRange(numPartitions, "number of partitions", 1, null);
    }

    @Override
    public Trigger build(String namespace, String applicationName, String applicationVersion) {
      return new PartitionTrigger(namespace, datasetName, numPartitions);
    }
  }

  /**
   * Abstract base class for composite trigger in an app.
   */
  public abstract static class AbstractCompositeTrigger extends ScheduleTrigger {
    protected final Trigger[] triggers;

    public AbstractCompositeTrigger(Type type, Trigger... triggers) {
      super(type);
      this.triggers = triggers;
      validate();
    }

    public Trigger[] getTriggers() {
      return triggers;
    }

    @Override
    public void validate() {
      if (!getType().isComposite()) {
        throw new IllegalArgumentException("Trigger type " + getType().name() + " is not a composite trigger.");
      }
      Trigger[] internalTriggers = getTriggers();
      validateNotNull(internalTriggers, "internal trigger");
      if (internalTriggers.length == 0) {
        throw new IllegalArgumentException(String.format("Triggers passed in to construct a trigger " +
                                                           "of type %s cannot be empty.", getType().name()));
      }
      for (Trigger trigger : internalTriggers) {
        if (trigger == null) {
          throw new IllegalArgumentException(String.format("Triggers passed in to construct a trigger " +
                                                             "of type %s cannot contain null.", getType().name()));
        }
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      AbstractCompositeTrigger that = (AbstractCompositeTrigger) o;
      return Arrays.equals(triggers, that.triggers);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(triggers);
    }
  }

  /**
   * Shorthand helper method to create an instance of {@link AndTriggerBuilder}
   */
  public static AndTriggerBuilder and(Trigger... triggers) {
    return new AndTriggerBuilder(triggers);
  }

  /**
   * Shorthand helper method to create an instance of {@link OrTriggerBuilder}
   */
  public static OrTriggerBuilder or(Trigger... triggers) {
    return new OrTriggerBuilder(triggers);
  }

  /**
   * A Trigger builder that builds a {@link AndTrigger}.
   */
  public static class AndTriggerBuilder extends AbstractCompositeTrigger implements TriggerBuilder {

    public AndTriggerBuilder(Trigger... triggers) {
      super(Type.AND, triggers);
    }

    @Override
    public Trigger build(String namespace, String applicationName, String applicationVersion) {
      int numTriggers = getTriggers().length;
      Trigger[] builtTriggers = new Trigger[numTriggers];
      for (int i = 0; i < numTriggers; i++) {
        Trigger trigger = getTriggers()[i];
        builtTriggers[i] = trigger instanceof TriggerBuilder ?
          ((TriggerBuilder) trigger).build(namespace, applicationName, applicationVersion) : trigger;
      }
      return new AndTrigger(builtTriggers);
    }
  }

  /**
   * Represents an AND trigger in an app.
   */
  public static class AndTrigger extends AbstractCompositeTrigger {
    public AndTrigger(Trigger... triggers) {
      super(Type.AND, triggers);
    }
  }

  /**
   * A Trigger builder that builds a {@link AndTrigger}.
   */
  public static class OrTriggerBuilder extends AbstractCompositeTrigger implements TriggerBuilder {

    public OrTriggerBuilder(Trigger... triggers) {
      super(Type.OR, triggers);
    }

    @Override
    public Trigger build(String namespace, String applicationName, String applicationVersion) {
      int numTriggers = getTriggers().length;
      Trigger[] builtTriggers = new Trigger[numTriggers];
      for (int i = 0; i < numTriggers; i++) {
        Trigger trigger = getTriggers()[i];
        builtTriggers[i] = trigger instanceof TriggerBuilder ?
          ((TriggerBuilder) trigger).build(namespace, applicationName, applicationVersion) : trigger;
      }
      return new OrTrigger(builtTriggers);
    }
  }

  /**
   * Represents an OR trigger in an app.
   */
  public static class OrTrigger extends AbstractCompositeTrigger {
    public OrTrigger(Trigger... triggers) {
      super(Type.OR, triggers);
    }
  }

  /**
   * Represents a stream size trigger in an app.
   */
  public static class StreamSizeTrigger extends ScheduleTrigger {

    private final String streamNamespace;
    private final String streamName;
    private final int triggerMB;

    public StreamSizeTrigger(String streamNamespace, String streamName, int triggerMB) {
      super(Type.STREAM_SIZE);
      this.streamNamespace = streamNamespace;
      this.streamName = streamName;
      this.triggerMB = triggerMB;
      validate();
    }

    public String getStreamNamespace() {
      return streamNamespace;
    }

    public String getStreamName() {
      return streamName;
    }
    
    public int getTriggerMB() {
      return triggerMB;
    }

    @Override
    public void validate() {
      validateNotNull(getStreamNamespace(), "stream namespace");
      validateNotNull(getStreamName(), "stream name");
      validateInRange(getTriggerMB(), "trigger in MB", 1, null);
    }

    @Override
    public boolean equals(Object o) {
      return this == o ||
        o != null &&
          getClass().equals(o.getClass()) &&
          Objects.equals(getStreamNamespace(), ((StreamSizeTrigger) o).getStreamNamespace()) &&
          Objects.equals(getStreamName(), ((StreamSizeTrigger) o).getStreamName()) &&
          Objects.equals(getTriggerMB(), ((StreamSizeTrigger) o).getTriggerMB());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getStreamNamespace(), getStreamName(), getTriggerMB());
    }

    @Override
    public String toString() {
      return String.format("StreamSizeTrigger(%s.%s, %d MB)", getStreamNamespace(), getStreamName(), getTriggerMB());
    }
  }

  /**
   * A Trigger builder that builds a {@link StreamSizeTrigger}.
   */
  public static class StreamSizeTriggerBuilder extends ScheduleTrigger implements TriggerBuilder {

    private final String streamName;
    private final int triggerMB;

    public StreamSizeTriggerBuilder(String streamName, int triggerMB) {
      super(Type.STREAM_SIZE);
      this.streamName = streamName;
      this.triggerMB = triggerMB;
      validate();
    }

    @Override
    public void validate() {
      validateNotNull(streamName, "stream name");
      validateInRange(triggerMB, "trigger in MB", 1, null);
    }

    @Override
    public Trigger build(String namespace, String applicationName, String applicationVersion) {
      return new StreamSizeTrigger(namespace, streamName, triggerMB);
    }
  }

  /**
   * Represents a program status trigger for an app
   */
  public static class ProgramStatusTrigger extends ScheduleTrigger {
    private final String programNamespace;
    private final String programApplication;
    private final String programApplicationVersion;
    private final ProgramType programType;
    private final String programName;
    protected final Set<ProgramStatus> programStatuses;

    public ProgramStatusTrigger(String programNamespace, String programApplication, String programApplicationVersion,
                                ProgramType programType, String programName, Set<ProgramStatus> programStatuses) {
      super(Type.PROGRAM_STATUS);
      this.programNamespace = programNamespace;
      this.programApplication = programApplication;
      this.programApplicationVersion = programApplicationVersion;
      this.programType = programType;
      this.programName = programName;
      this.programStatuses = programStatuses;
      validate();
    }

    public String getProgramNamespace() {
      return programNamespace;
    }

    public String getProgramApplication() {
      return programApplication;
    }

    public String getProgramApplicationVersion() {
      return programApplicationVersion;
    }

    public ProgramType getProgramType() {
      return programType;
    }

    public String getProgramName() {
      return programName;
    }

    public Set<ProgramStatus> getProgramStatuses() {
      return programStatuses;
    }

    @Override
    public void validate() {
      if (getProgramStatuses().contains(ProgramStatus.INITIALIZING) ||
        getProgramStatuses().contains(ProgramStatus.RUNNING)) {
        throw new IllegalArgumentException(String.format(
          "Cannot allow triggering program %s with status %s: COMPLETED, FAILED, KILLED statuses are supported",
          getProgramName(), getProgramType()));
      }

      validateNotNull(getProgramNamespace(), "namespace");
      validateNotNull(getProgramApplication(), "application name");
      validateNotNull(getProgramApplicationVersion(), "application version");
      validateNotNull(getProgramName(), "program name");
      validateNotNull(getProgramStatuses(), "program statuses");
    }

    @Override
    public int hashCode() {
      return Objects.hash(getProgramNamespace(), getProgramApplication(), getProgramApplicationVersion(),
                          getProgramName(), getProgramStatuses());
    }

    @Override
    public boolean equals(Object o) {
      return this == o ||
        o != null &&
          getClass().equals(o.getClass()) &&
          Objects.equals(getProgramNamespace(), ((ProgramStatusTrigger) o).getProgramNamespace()) &&
          Objects.equals(getProgramApplication(), ((ProgramStatusTrigger) o).getProgramApplication()) &&
          Objects.equals(getProgramApplicationVersion(), ((ProgramStatusTrigger) o).getProgramApplicationVersion()) &&
          Objects.equals(getProgramName(), ((ProgramStatusTrigger) o).getProgramName()) &&
          Objects.equals(getProgramStatuses(), ((ProgramStatusTrigger) o).getProgramStatuses());
    }

    @Override
    public String toString() {
      return String.format("ProgramStatusTrigger(%s.%s.%s.%s, %s)", getProgramNamespace(), getProgramApplication(),
                           getProgramApplicationVersion(), getProgramName(), getProgramStatuses().toString());
    }
  }

  /**
   * A Trigger builder that builds a {@link ProgramStatusTrigger}.
   */
  public static class ProgramStatusTriggerBuilder implements TriggerBuilder {
    private final String programNamespace;
    private final String programApplication;
    private final String programApplicationVersion;
    private final ProgramType programType;
    private final String programName;
    private final EnumSet<ProgramStatus> programStatuses;

    public ProgramStatusTriggerBuilder(@Nullable String programNamespace, @Nullable String programApplication,
                                       @Nullable String programApplicationVersion, String programType,
                                       String programName, ProgramStatus... programStatuses) {
      this.programNamespace = programNamespace;
      this.programApplication = programApplication;
      this.programApplicationVersion = programApplicationVersion;
      this.programType = ProgramType.valueOf(programType);
      this.programName = programName;

      // User can not specify any program statuses, or specify null, which is an array of length 1 containing null
      if (programStatuses.length == 0 || (programStatuses.length == 1 && programStatuses[0] == null)) {
        throw new IllegalArgumentException("Must set a program state for the triggering program");
      }
      this.programStatuses = EnumSet.of(programStatuses[0], programStatuses);
    }

    @Override
    public ProgramStatusTrigger build(String namespace, String applicationName, String applicationVersion) {
      // Inherit environment attributes from the deployed application
      return new ProgramStatusTrigger(firstNonNull(programNamespace, namespace),
                                      firstNonNull(programApplication, applicationName),
                                      firstNonNull(programApplicationVersion, applicationVersion),
                                      programType, programName, programStatuses);
    }
  }

  private static String firstNonNull(@Nullable String first, String second) {
    if (first != null) {
      return first;
    }
    return second;
  }

  private static void validateNotNull(@Nullable Object o, String name) {
    if (o == null) {
      throw new IllegalArgumentException(name + " must not be null");
    }
  }

  private static <V extends Comparable<V>>
  void validateInRange(@Nullable V value, String name, @Nullable V minValue, @Nullable V maxValue) {
    if (value == null) {
      throw new IllegalArgumentException(name + " must be specified");
    }
    if (minValue != null && value.compareTo(minValue) < 0) {
      throw new IllegalArgumentException(name + " must be greater than or equal to" + minValue + " but is " + value);
    }
    if (maxValue != null && value.compareTo(maxValue) > 0) {
      throw new IllegalArgumentException(name + " must be less than or equal to " + maxValue + " but is " + value);
    }
  }
}


