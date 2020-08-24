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
package io.prestosql.testng.services;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;
import org.testng.IRetryAnalyzer;
import org.testng.ITestNGMethod;
import org.testng.ITestResult;

import javax.annotation.concurrent.GuardedBy;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

public class FlakyTestRetryAnalyzer
        implements IRetryAnalyzer
{
    private static final Logger log = Logger.get(FlakyTestRetryAnalyzer.class);

    // This property exists so that flaky tests are retried on CI only by default but tests of retrying pass locally as well.
    // TODO replace pom.xml property with explicit invocation of a testng runner (test suite with a test) and amend the retryer behavior on that level
    private static final String ENABLE_PROPERTY = "io.prestosql.testng.services.FlakyTestRetryAnalyzer.enabled";

    @VisibleForTesting
    static final int ALLOWED_RETRIES_COUNT = 2;

    @GuardedBy("this")
    private final Map<String, Long> retryCounter = new HashMap<>();

    @Override
    public boolean retry(ITestResult result)
    {
        if (!isEnabled()) {
            return false;
        }
        if (!isTestRetryable(result)) {
            return false;
        }
        long retryCount;
        ITestNGMethod method = result.getMethod();
        synchronized (this) {
            String name = getName(method, result.getParameters());
            retryCount = retryCounter.getOrDefault(name, 0L);
            retryCount++;
            if (retryCount > ALLOWED_RETRIES_COUNT) {
                return false;
            }
            retryCounter.put(name, retryCount);
        }
        log.warn(
                result.getThrowable(),
                "Test %s::%s attempt %s failed, retrying...,",
                result.getTestClass().getName(),
                method.getMethodName(),
                retryCount);
        return true;
    }

    private static boolean isEnabled()
    {
        if (System.getProperty(ENABLE_PROPERTY) != null) {
            return Boolean.getBoolean(ENABLE_PROPERTY);
        }

        // Enable retry on CI by default
        return System.getenv("CONTINUOUS_INTEGRATION") != null;
    }

    private static boolean isTestRetryable(ITestResult result)
    {
        Method method = result.getMethod().getConstructorOrMethod().getMethod();
        if (method == null) {
            return false;
        }
        return method.getAnnotation(Flaky.class) != null;
    }

    private static String getName(ITestNGMethod method, Object[] parameters)
    {
        String actualTestClass = method.getTestClass().getName();
        if (parameters.length != 0) {
            return format(
                    "%s::%s(%s)",
                    actualTestClass,
                    method.getMethodName(),
                    String.join(",", Stream.of(parameters).map(Object::toString).collect(toImmutableList())));
        }
        return format("%s::%s", actualTestClass, method.getMethodName());
    }
}