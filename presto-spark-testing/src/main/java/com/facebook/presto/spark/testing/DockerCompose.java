/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spark.testing;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spark.testing.Processes.startProcess;
import static com.facebook.presto.spark.testing.Processes.waitForProcess;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DockerCompose
{
    private final File composeYaml;

    public DockerCompose(File composeYaml)
    {
        requireNonNull(composeYaml, "composeYaml is null");
        checkArgument(composeYaml.exists() && composeYaml.isFile(), "file does not exist: %s", composeYaml);
        checkArgument(composeYaml.canRead(), "file is not readable: %s", composeYaml);
        this.composeYaml = composeYaml;
    }

    public void verifyInstallation()
            throws InterruptedException
    {
        checkState(Processes.execute("docker", "--version") == 0, "docker is not installed");
        checkState(Processes.execute("docker-compose", "--version") == 0, "docker-compose is not installed");
    }

    public void pull()
            throws InterruptedException
    {
        int exitCode = execute("pull");
        checkState(exitCode == 0, "pull existed with code: %s", exitCode);
    }

    public Process up(Map<String, Integer> services)
    {
        ImmutableList.Builder<String> parameters = ImmutableList.builder();
        parameters.add("up", "--force-recreate", "--always-recreate-deps", "--abort-on-container-exit");
        services.forEach((service, scale) -> {
            parameters.add("--scale", format("%s=%s", service, scale));
        });
        parameters.addAll(services.keySet());
        return start(parameters.build());
    }

    public void down()
            throws InterruptedException
    {
        int exitCode = execute("down");
        checkState(exitCode == 0, "down existed with code: %s", exitCode);
    }

    public String getContainerAddress(String service)
            throws InterruptedException
    {
        String containerId = getContainerId(service);
        return Processes.executeForOutput("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", containerId).trim();
    }

    private String getContainerId(String service)
            throws InterruptedException
    {
        List<String> containerIds = getContainerIds();
        for (String containerId : containerIds) {
            String output = Processes.executeForOutput(
                    "docker",
                    "ps", "-q",
                    "--filter", format("id=%s", containerId),
                    "--filter", format("name=%s", service));
            if (!output.isEmpty()) {
                return containerId;
            }
        }
        throw new IllegalArgumentException(format("container not found: %s", service));
    }

    private List<String> getContainerIds()
            throws InterruptedException
    {
        String output = executeForOutput("ps", "-q");
        return Splitter.on('\n').trimResults().omitEmptyStrings().splitToList(output);
    }

    public int execute(String... args)
            throws InterruptedException
    {
        return execute(ImmutableList.copyOf(args));
    }

    public int execute(List<String> args)
            throws InterruptedException
    {
        return waitForProcess(start(args));
    }

    public String executeForOutput(String... args)
            throws InterruptedException
    {
        return executeForOutput(ImmutableList.copyOf(args));
    }

    public String executeForOutput(List<String> args)
            throws InterruptedException
    {
        return Processes.executeForOutput(ImmutableList.<String>builder()
                .add("docker-compose", "-f", composeYaml.getAbsolutePath())
                .addAll(args)
                .build());
    }

    public Process start(List<String> args)
    {
        return startProcess(ImmutableList.<String>builder()
                .add("docker-compose", "-f", composeYaml.getAbsolutePath())
                .addAll(args)
                .build());
    }
}
