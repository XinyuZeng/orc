/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "ToolsHelper.hh"

#include <iostream>
#include <chrono>
#include "Stats.hh"
#include "Config.hh"

void scanFile(std::ostream & out, const char* filename, uint64_t batchSize,
              const orc::RowReaderOptions& rowReaderOpts) {
  orc::ReaderOptions readerOpts;
  std::unique_ptr<orc::Reader> reader =
    orc::createReader(orc::readFile(filename), readerOpts);
  std::unique_ptr<orc::RowReader> rowReader = reader->createRowReader(rowReaderOpts);

  std::vector<std::unique_ptr<orc::ColumnVectorBatch>> result_batches;
  auto begin = std::chrono::steady_clock::now();

  unsigned long rows = 0;
  unsigned long batches = 0;
  bool got_vals = false;
  do {
    std::unique_ptr<orc::ColumnVectorBatch> batch =
      rowReader->createRowBatch(batchSize);
    got_vals = rowReader->next(*batch);
    if (got_vals) {
      batches += 1;
      rows += batch->numElements;
      result_batches.push_back(std::move(batch));
    }
  } while(got_vals);
  out << "read orc(s): " << (static_cast<std::chrono::duration<double>>(std::chrono::steady_clock::now() - begin)).count() <<std::endl;
  out << "Rows: " << rows << std::endl;
  out << "Batches: " << batches << std::endl;
}

int main(int argc, char* argv[]) {
  uint64_t batchSize = 1024;
  orc::RowReaderOptions rowReaderOptions;
  bool success = parseOptions(&argc, &argv, &batchSize, &rowReaderOptions);
  if (argc < 1 || !success) {
    std::cerr << "Usage: orc-scan [options] <filename>...\n";
    printOptions(std::cerr);
    std::cerr << "Scans and displays the row count of the ORC files.\n";
    return 1;
  }
  for (int i = 0; i < argc; ++i) {
    try {
      scanFile(std::cout, argv[i], batchSize, rowReaderOptions);
    } catch (std::exception& ex) {
      std::cerr << "Caught exception in " << argv[i] << ": " << ex.what() << "\n";
      return 1;
    }
  }
  std::cout << "total levels time: " << orc::time_levels << "ns" << std::endl;
  std::cout << "total decode time: " << orc::time_decode << "ns" << std::endl;
  #if ORC_STATS_ENABLE
  std::cout << "total read time: " << orc::time_read << "ns" << std::endl;
  std::cout << "total read cnt: " << orc::num_read << std::endl;
  #endif
  return 0;
}
