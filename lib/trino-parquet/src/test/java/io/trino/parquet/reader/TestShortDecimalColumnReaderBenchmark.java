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
package io.trino.parquet.reader;

import com.google.common.collect.ImmutableList;
import io.trino.parquet.ParquetEncoding;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.trino.parquet.ParquetEncoding.DELTA_BYTE_ARRAY;
import static io.trino.parquet.ParquetEncoding.PLAIN;

public class TestShortDecimalColumnReaderBenchmark
{
    @Test
    public void testShortDecimalColumnReaderBenchmark()
            throws IOException
    {
        for (int typeLength = 1; typeLength <= 8; typeLength++) {
            for (ParquetEncoding encoding : ImmutableList.of(PLAIN, DELTA_BYTE_ARRAY)) {
                BenchmarkShortDecimalColumnReader benchmark = new BenchmarkShortDecimalColumnReader();
                benchmark.byteArrayLength = typeLength;
                benchmark.encoding = encoding;
                benchmark.setup();
                benchmark.read();
            }
        }
    }
}
