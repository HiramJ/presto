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
package com.facebook.presto.dht.filters;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;

public class TestColumnBasedFilter
{
    @Test
    public void testToBytes()
            throws NoSuchFieldException
    {
        List<Integer> colums = ImmutableList.of(1, 6, 12);
        ColumnBasedFilter filter = new ColumnBasedFilter(colums);

        byte[] filterBytes = filter.toBytes();
        filter = new ColumnBasedFilter(filterBytes);
        assertEquals(Arrays.stream(filter.columns).boxed().collect(Collectors.toList()), colums);
    }
}
