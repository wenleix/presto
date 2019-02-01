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
package com.facebook.presto.sql.planner;

import static java.lang.String.format;
import static java.lang.System.identityHashCode;
import static java.util.Objects.requireNonNull;

public class Reference<T>
{
    private final T reference;

    public static <T> Reference<T> of(T reference)
    {
        return new Reference<>(reference);
    }

    public Reference(T reference)
    {
        this.reference = requireNonNull(reference, "reference is null");
    }

    public T get()
    {
        return reference;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Reference that = (Reference) o;
        return this.reference == that.reference;
    }

    @Override
    public int hashCode()
    {
        return identityHashCode(reference);
    }

    @Override
    public String toString()
    {
        return format(
                "@%s: %s",
                Integer.toHexString(identityHashCode(reference)),
                reference);
    }
}