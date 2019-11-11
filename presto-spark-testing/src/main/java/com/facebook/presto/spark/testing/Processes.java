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

import com.facebook.airlift.log.Logger;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;
import io.airlift.units.Duration;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.ProcessBuilder.Redirect.PIPE;
import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Processes
{
    private static final Logger log = Logger.get(Processes.class);

    private Processes() {}

    public static String executeForOutput(String... command)
            throws InterruptedException
    {
        return executeForOutput(ImmutableList.copyOf(command));
    }

    public static String executeForOutput(List<String> command)
            throws InterruptedException
    {
        log.info("Running: %s", Joiner.on(" ").join(command));
        ProcessBuilder processBuilder = createProcessBuilder(command);
        processBuilder.redirectOutput(PIPE);
        Process process = startProcess(processBuilder);
        String result;
        try (InputStream inputStream = process.getInputStream()) {
            result = CharStreams.toString(new InputStreamReader(inputStream, UTF_8));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        int exitCode = waitForProcess(process);
        checkState(exitCode == 0, "[%s] process existed with status: %s", Joiner.on(" ").join(command), exitCode);
        return result;
    }

    public static int execute(String... command)
            throws InterruptedException
    {
        return execute(ImmutableList.copyOf(command));
    }

    public static int execute(List<String> command)
            throws InterruptedException
    {
        log.info("Running: %s", Joiner.on(" ").join(command));
        return waitForProcess(startProcess(command));
    }

    public static int waitForProcess(Process process)
            throws InterruptedException
    {
        try {
            return process.waitFor();
        }
        catch (InterruptedException e) {
            destroyProcess(process);
            throw e;
        }
    }

    public static Process startProcess(List<String> command)
    {
        log.info("Starting: %s", Joiner.on(" ").join(command));
        ProcessBuilder processBuilder = createProcessBuilder(command);
        return startProcess(processBuilder);
    }

    public static Process startProcess(ProcessBuilder processBuilder)
    {
        try {
            return processBuilder.start();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void ensureProcessIsRunning(Process process, Duration duration)
            throws InterruptedException
    {
        Stopwatch stopwatch = Stopwatch.createStarted();
        while (stopwatch.elapsed(SECONDS) < duration.roundTo(SECONDS)) {
            if (!process.isAlive()) {
                throw new IllegalStateException(format("process existed with status: %s", process.waitFor()));
            }
            sleep(duration.toMillis());
        }
    }

    public static void destroyProcess(Process process)
    {
        if (!process.isAlive()) {
            return;
        }

        // stop
        process.destroy();

        // wait for 5 seconds
        for (int i = 0; i < 5; i++) {
            if (!process.isAlive()) {
                break;
            }
            sleepUninterruptibly(1, SECONDS);
        }

        // kill
        process.destroyForcibly();
    }

    private static ProcessBuilder createProcessBuilder(List<String> command)
    {
        return new ProcessBuilder(command)
                .inheritIO();
    }
}
