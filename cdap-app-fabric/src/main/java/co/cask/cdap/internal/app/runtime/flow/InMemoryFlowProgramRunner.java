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

package co.cask.cdap.internal.app.runtime.flow;

import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.internal.app.AbstractInMemoryProgramRunner;
import com.google.inject.Inject;
import com.google.inject.Provider;

/**
 * For running {@link FlowProgramRunner}. Only used in-memory / standalone.
 */
public final class InMemoryFlowProgramRunner extends AbstractInMemoryProgramRunner {

  @Inject
  public InMemoryFlowProgramRunner(CConfiguration cConf, ProgramStateWriter programStateWriter,
                                   Provider<FlowProgramRunner> flowProgramRunnerProvider) {
    super(cConf, flowProgramRunnerProvider.get(), programStateWriter);
  }
}

