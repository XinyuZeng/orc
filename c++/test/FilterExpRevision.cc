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
#include <list>
#include <string>
#include <chrono>
#include <string_view>
#include <algorithm>
#include <sstream>

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

// std::list<std::string> cols = {"key5790", "key3806", "key4339", "key1858", "key8846", "key1239", "key6409", "key1661", "key5345", "key3646", "key3319", "key863", "key7557", "key1207", "key6747", "key4304", "key223", "key6671", "key2410", "key7677", "key6178", "key1151", "key5588", "key952", "key673", "key8793", "key205", "key8885", "key6530", "key2676", "key692", "key5885", "key9994", "key4584", "key1386", "key1912", "key5026", "key5659", "key2854", "key2540", "key5033", "key8240", "key3478", "key3190", "key4918", "key2029", "key8055", "key8761", "key4818", "key1615", "key9756", "key6935", "key2506", "key1423", "key7306", "key1047", "key7826", "key7560", "key8120", "key9562", "key4458", "key9541", "key9680", "key1897", "key6082", "key8370", "key7240", "key6527", "key7092", "key7776", "key9104", "key215", "key9866", "key357", "key694", "key763", "key8613", "key8179", "key718", "key3457", "key6873", "key5754", "key7522", "key9958", "key7447", "key608", "key5871", "key6667", "key3919", "key2673", "key2166", "key325", "key7411", "key2061", "key439", "key4374", "key5571", "key1090", "key7997", "key3120", "val5790", "val3806", "val4339", "val1858", "val8846", "val1239", "val6409", "val1661", "val5345", "val3646", "val3319", "val863", "val7557", "val1207", "val6747", "val4304", "val223", "val6671", "val2410", "val7677", "val6178", "val1151", "val5588", "val952", "val673", "val8793", "val205", "val8885", "val6530", "val2676", "val692", "val5885", "val9994", "val4584", "val1386", "val1912", "val5026", "val5659", "val2854", "val2540", "val5033", "val8240", "val3478", "val3190", "val4918", "val2029", "val8055", "val8761", "val4818", "val1615", "val9756", "val6935", "val2506", "val1423", "val7306", "val1047", "val7826", "val7560", "val8120", "val9562", "val4458", "val9541", "val9680", "val1897", "val6082", "val8370", "val7240", "val6527", "val7092", "val7776", "val9104", "val215", "val9866", "val357", "val694", "val763", "val8613", "val8179", "val718", "val3457", "val6873", "val5754", "val7522", "val9958", "val7447", "val608", "val5871", "val6667", "val3919", "val2673", "val2166", "val325", "val7411", "val2061", "val439", "val4374", "val5571", "val1090", "val7997", "val3120"};
std::list<std::string> cols;

// Used by projection (ML training) experiment
int main(int argc, char **argv)
{
  if (argc != 8 && argc != 9) {
    std::cout << "Usage: " << argv[0] << " <file_name> <col_name> <data_type> <filter_type> <v1> <v2> <proj_type>" << std::endl;
    return 1;
  }
  std::string file_name = argv[1];
  // argv[2] is a list of strings separate by comma
  std::stringstream ss(argv[2]);
  std::string token;
  while (std::getline(ss, token, ',')) {
      cols.push_back(token);
  }
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
    rowReaderOpts.include(cols);
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
  std::cout << "startNextStripe (us): " << time_startNextStripe << std::endl;
  std::cout << "buildReader (us): " << time_buildReader << std::endl;
  std::cout << "total read:" << cnt << std::endl;
  system("cat /proc/$PPID/io");
  return 0;
}