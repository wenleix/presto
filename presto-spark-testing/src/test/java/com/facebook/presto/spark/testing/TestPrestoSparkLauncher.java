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
import io.airlift.units.Duration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Paths;

import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.toByteArray;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestPrestoSparkLauncher
{
    private static final Logger log = Logger.get(TestPrestoSparkLauncher.class);

    private File tempDir;
    private Process composeProcess;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        verifyDockerInstallation();

        tempDir = createTempDir();
        tempDir.deleteOnExit();

        File composeYaml = extractResource("docker-compose.yml", tempDir);
        dockerComposePull(composeYaml);
        composeProcess = dockerComposeStart(composeYaml);
        ensureRunning(composeProcess, new Duration(1, MINUTES));
    }

    @AfterClass
    public void tearDown()
            throws Exception
    {
        if (tempDir != null) {
            tempDir.delete();
        }
        if (composeProcess != null) {
            destroy(composeProcess);
        }
    }

    private static void verifyDockerInstallation()
    {
        log.info("Verifying docker installation:");
        assertEquals(execute("docker", "--version"), 0);
        assertEquals(execute("docker-compose", "--version"), 0);
    }

    private static int dockerComposePull(File composeYaml)
    {
        return execute("docker-compose", "-f", composeYaml.getAbsolutePath(), "pull");
    }

    private static Process dockerComposeStart(File composeYaml)
    {
        return startProcess("docker-compose",
                "-f", composeYaml.getAbsolutePath(),
                "up",
                "--scale", "spark-worker=3",
                "--force-recreate",
                "--always-recreate-deps",
                "--abort-on-container-exit");
    }

    private static int execute(String... command)
    {
        log.info("Running: %s", Joiner.on(" ").join(command));
        Process process = startProcess(command);
        try {
            return process.waitFor();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            destroy(process);
            throw new RuntimeException(e);
        }
    }

    private static Process startProcess(String... command)
    {
        log.info("Starting: %s", Joiner.on(" ").join(command));
        ProcessBuilder processBuilder = new ProcessBuilder(command)
                .inheritIO();
        return startProcess(processBuilder);
    }

    private static void ensureRunning(Process process, Duration duration)
    {
        Stopwatch stopwatch = Stopwatch.createStarted();
        while (stopwatch.elapsed(SECONDS) < duration.roundTo(SECONDS)) {
            assertTrue(process.isAlive());
            sleepUninterruptibly(1, SECONDS);
        }
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

    public static void destroy(Process process)
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

    private static File extractResource(String resource, File destinationDirectory)
    {
        File file = new File(destinationDirectory, Paths.get(resource).getFileName().toString());
        try (FileOutputStream outputStream = new FileOutputStream(file)) {
            outputStream.write(toByteArray(getResource(resource)));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return file;
    }

    @Test
    public void test()
            throws Exception
    {
        Thread.sleep(10000);
    }
}
