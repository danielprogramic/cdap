/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.procedure;

import co.cask.cdap.api.metrics.Metrics;
import co.cask.cdap.api.procedure.ProcedureContext;
import co.cask.cdap.api.procedure.ProcedureSpecification;
import co.cask.cdap.app.metrics.ProcedureMetrics;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.AbstractContext;
import co.cask.cdap.logging.context.ProcedureLoggingContext;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Procedure runtime context
 */
final class BasicProcedureContext extends AbstractContext implements ProcedureContext {

  private final String accountId;
  private final String procedureId;
  private final int instanceId;
  private volatile int instanceCount;

  private final ProcedureSpecification procedureSpec;
  private final ProcedureMetrics procedureMetrics;
  private final ProcedureLoggingContext procedureLoggingContext;
  private final Arguments runtimeArguments;

  BasicProcedureContext(Program program, RunId runId, int instanceId, int instanceCount,
                        Set<String> datasets, Arguments runtimeArguments,
                        ProcedureSpecification procedureSpec, MetricsCollectionService collectionService,
                        DiscoveryServiceClient discoveryServiceClient,
                        DatasetFramework dsFramework, CConfiguration conf) {
    super(program, runId, datasets,
          getMetricsContext(program, instanceId), collectionService,
          dsFramework, conf, discoveryServiceClient);
    this.accountId = program.getAccountId();
    this.procedureId = program.getName();
    this.instanceId = instanceId;
    this.instanceCount = instanceCount;
    this.procedureSpec = procedureSpec;
    this.procedureMetrics = new ProcedureMetrics(collectionService, getApplicationId(), getProcedureId());
    this.runtimeArguments = runtimeArguments;
    this.procedureLoggingContext = new ProcedureLoggingContext(getAccountId(), getApplicationId(), getProcedureId());
  }

  @Override
  public String toString() {
    return String.format("procedure=%s, instance=%d, %s", getProcedureId(), getInstanceId(), super.toString());
  }

  @Override
  public ProcedureSpecification getSpecification() {
    return procedureSpec;
  }

  @Override
  public Metrics getMetrics() {
    return procedureMetrics;
  }

  @Override
  public int getInstanceCount() {
    return instanceCount;
  }

  public String getProcedureId() {
    return procedureId;
  }

  public int getInstanceId() {
    return instanceId;
  }

  public LoggingContext getLoggingContext() {
    return procedureLoggingContext;
  }

  private static String getMetricsContext(Program program, int instanceId) {
    return String.format("%s.p.%s.%d",
                         program.getApplicationId(),
                         // procedure id
                         program.getName(),
                         instanceId);
  }

  public void setInstanceCount(int count) {
    instanceCount = count;
  }

  /**
   * @return A map of argument key and value.
   */
  @Override
  public Map<String, String> getRuntimeArguments() {
    ImmutableMap.Builder<String, String> arguments = ImmutableMap.builder();
    Iterator<Map.Entry<String, String>> it = runtimeArguments.iterator();
    while (it.hasNext()) {
      arguments.put(it.next());
    }
    return arguments.build();
  }
}
