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
#include <fstream>
#include <iostream>
#include <string>
#include <chrono>
#include <string_view>
#include <algorithm>

#include "Adaptor.hh"
#include "MemoryInputStream.hh"
#include "MemoryOutputStream.hh"
#include "orc/OrcFile.hh"
#include "orc/sargs/SearchArgument.hh"
#include "orc/ColumnPrinter.hh"
#include "Stats.hh"
// #include "stats.h"
// #include "wrap/orc-proto-wrapper.hh"

using namespace orc;


int main(int argc, char **argv)
{
  if (argc != 8 && argc != 9) {
    std::cout << "Usage: " << argv[0] << " <file_name> <col_name> <data_type> <filter_type> <v1> <v2> <proj_type>" << std::endl;
    return 1;
  }
  std::string file_name = argv[1];
  std::string col_name = argv[2];
  std::string data_type = argv[3]; // int, float, string
  std::string filter_type = argv[4]; // point, range,none
  std::string v1 = argv[5]; // 
  std::string v2 = argv[6]; // 
  std::string proj_type = argv[7];
  bool eval = false;
  if (argc >= 9) {
    eval = true;
  }

  std::vector<bool> select_vec = std::vector<bool>(1024, false);
  double v1d, v2d;
  int64_t v1i, v2i;
  std::string v1s, v2s;
  if (data_type == "float") {
    v1d = std::atof(v1.c_str());
    v2d = std::atof(v2.c_str());
  } else if (data_type == "int") {
    v1i = std::atoll(v1.c_str());
    v2i = std::atoll(v2.c_str());
  } else if (data_type == "string") {
    v1s = v1;
    v2s = v2;
  } else {
    std::cout << "data_type must be int, float, or string" << std::endl;
    return 1;
  }

  auto begin = std::chrono::steady_clock::now();
  ReaderOptions readerOpts;
  std::unique_ptr<Reader> reader =
      createReader(readLocalFile(file_name), readerOpts);
  RowReaderOptions rowReaderOpts;
  if (proj_type == "all") {
    assert(true);
  } else if (proj_type == "one") {
    rowReaderOpts.include({col_name});
  } else {
    std::cout << "proj_type must be all or one" << std::endl;
    return 1;
  }
  std::unique_ptr<SearchArgument> sarg;
  if (filter_type == "none") {
    assert(true);
  } else if (filter_type == "point") {
    if (data_type == "int") {
      sarg = SearchArgumentFactory::newBuilder()
          ->startAnd()
          .equals(col_name, PredicateDataType::LONG, Literal(static_cast<int64_t>(std::atoll(v1.c_str()))))
          .end()
          .build();
    } else if (data_type == "float") {
      sarg = SearchArgumentFactory::newBuilder()
          ->startAnd()
          .equals(col_name, PredicateDataType::FLOAT, Literal(static_cast<double>(std::atof(v1.c_str()))))
          .end()
          .build();
    } else if (data_type == "string") {
      sarg = SearchArgumentFactory::newBuilder()
          ->startAnd()
          .equals(col_name, PredicateDataType::STRING, Literal(v1.c_str(), v1.size()))
          .end()
          .build();
    }
  } else if (filter_type == "range") {
    if (data_type == "int") {
      sarg = SearchArgumentFactory::newBuilder()
          ->startAnd()
          .between(col_name, PredicateDataType::LONG, Literal(static_cast<int64_t>(std::atoll(v1.c_str()))), Literal(static_cast<int64_t>(std::atoll(v2.c_str()))))
          .end()
          .build();
    } else if (data_type == "float") {
      sarg = SearchArgumentFactory::newBuilder()
          ->startAnd()
          .between(col_name, PredicateDataType::FLOAT, Literal(static_cast<double>(std::atof(v1.c_str()))), Literal(static_cast<double>(std::atof(v2.c_str()))))
          .end()
          .build();
    } else if (data_type == "string") {
      sarg = SearchArgumentFactory::newBuilder()
          ->startAnd()
          .between(col_name, PredicateDataType::STRING, Literal(v1.c_str(), v1.size()), Literal(v2.c_str(), v2.size()))
          .end()
          .build();
    }
  }
  rowReaderOpts.searchArgument(std::move(sarg));
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto readBatch = rowReader->createRowBatch(1024);
  
  uint64_t cnt = 0;
  while (rowReader->next(*readBatch))
  {
    cnt += readBatch->numElements;
    if (eval) {
      auto struct_batch = dynamic_cast<StructVectorBatch *>(readBatch.get());
      if (data_type == "float"){
        auto double_batch = dynamic_cast<DoubleVectorBatch *>(struct_batch->fields[0]);
        for (int i = 0; i < readBatch->numElements; ++i) {
            double val = double_batch->data[i];
            if (val >= v1d && val <= v2d) {
              select_vec[i] = true;
            }else {
            select_vec[i] = false;
          }
        }
      } else if (data_type == "int") {
        auto long_batch = dynamic_cast<LongVectorBatch *>(struct_batch->fields[0]);
        for (int i = 0; i < readBatch->numElements; ++i) {
            int64_t val = long_batch->data[i];
            if (val >= v1i && val <= v2i) {
              select_vec[i] = true;
            }else {
            select_vec[i] = false;
          }
        }
      } else if (data_type == "string") {
        auto string_batch = dynamic_cast<StringVectorBatch *>(struct_batch->fields[0]);
        for (int i = 0; i < readBatch->numElements; ++i) {
          if (string_batch->hasNulls && string_batch->notNull[i] == false) {
            select_vec[i] = false;
            continue;
          }
          if (strcmp(string_batch->data[i], v1s.c_str()) >= 0 &&
              strcmp(string_batch->data[i], v2s.c_str()) <= 0) {
              select_vec[i] = true;
          } else {
            select_vec[i] = false;
          }
        }
      }
    }
  }

  std::cout << "read orc(s): " << (static_cast<std::chrono::duration<double>>(std::chrono::steady_clock::now() - begin)).count() <<std::endl;
  std::cout << "read meta (us): " << time_meta << std::endl;
  std::cout << "total read:" << cnt << std::endl;
  system("cat /proc/$PPID/io");
  return 0;
}