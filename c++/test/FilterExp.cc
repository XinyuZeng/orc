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
  // MemoryPool *pool = getDefaultPool();
  // assert(argc == 2);
  if (argc != 8) {
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
      // SearchArgumentFactory::newBuilder()
      //     ->startAnd()
      //     .equals(col_name, PredicateDataType::LONG, Literal(static_cast<int64_t>(std::atoll(filter_val.c_str()))))
        //   .equals(col_name, PredicateDataType::STRING, Literal(filter_val.c_str(), filter_val.size()))
          // .lessThan("l_quantity", PredicateDataType::LONG,
          //           Literal(static_cast<int64_t>(24L)))
        //   .between("l_shipdate", PredicateDataType::STRING,
        //            Literal(min_date.c_str(), 10), Literal(max_date.c_str(), 10))
          // .end()
          // .build();
  rowReaderOpts.searchArgument(std::move(sarg));
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto readBatch = rowReader->createRowBatch(1024);
  // EXPECT_EQ(true, rowReader->next(*readBatch));

  // NOTE: printer
  // std::string line;
  // std::unique_ptr<orc::ColumnPrinter> printer =
  //   createColumnPrinter(line, &rowReader->getSelectedType());
  
  uint64_t cnt = 0;
  while (rowReader->next(*readBatch))
  {
    // std::cout << "read out a batch, size:" << readBatch->numElements <<
    // std::endl;
    cnt += readBatch->numElements;
    // auto& batch0 = dynamic_cast<LongVectorBatch&>(*readBatch);
    // batch0.data
    // if (cnt == 9139168)
    // {
    //   std::cout << "read out 9139168 rows" << std::endl;
    // }
    // assert(true);

    // NOTE: printer
    // printer->reset(*readBatch);
    // for(unsigned long i=0; i < readBatch->numElements; ++i) {
    //   line.clear();
    //   printer->printRow(i);
    //   line += "\n";
    //   const char* str = line.c_str();
    //   fwrite(str, 1, strlen(str), stdout);
    // }
  }

  std::cout << "read orc(s): " << (static_cast<std::chrono::duration<double>>(std::chrono::steady_clock::now() - begin)).count() <<std::endl;
  std::cout << "read meta (us): " << time_meta << std::endl;
  std::cout << "total read:" << cnt << std::endl;
  system("cat /proc/$PPID/io");
  return 0;
}