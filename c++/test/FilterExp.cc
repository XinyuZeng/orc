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

#include "Adaptor.hh"
#include "MemoryInputStream.hh"
#include "MemoryOutputStream.hh"
#include "orc/OrcFile.hh"
#include "orc/sargs/SearchArgument.hh"
// #include "stats.h"
#include "wrap/orc-proto-wrapper.hh"

using namespace orc;
/**
 * Write an empty custom ORC file with a lot of control over format.
 * We use this to create files to test the reader rather than anything
 * that users would want to do.
 */
void writeCustomOrcFile(const std::string &filename,
                        const orc::proto::Metadata &metadata,
                        const orc::proto::Footer &footer,
                        const std::vector<std::uint32_t> &version,
                        std::uint32_t writerVersion)
{
  std::fstream output(filename.c_str(),
                      std::ios::out | std::ios::trunc | std::ios::binary);
  output << "ORC";
  if (!metadata.SerializeToOstream(&output))
  {
    std::cerr << "Failed to write metadata for " << filename << "\n";
    exit(1);
  }
  if (!footer.SerializeToOstream(&output))
  {
    std::cerr << "Failed to write footer for " << filename << "\n";
    exit(1);
  }
  orc::proto::PostScript ps;
  ps.set_footerlength(static_cast<uint64_t>(footer.ByteSizeLong()));
  ps.set_compression(orc::proto::NONE);
  ps.set_compressionblocksize(64 * 1024);
  for (size_t i = 0; i < version.size(); ++i)
  {
    ps.add_version(version[i]);
  }
  ps.set_metadatalength(static_cast<uint64_t>(metadata.ByteSizeLong()));
  ps.set_writerversion(writerVersion);
  ps.set_magic("ORC");
  if (!ps.SerializeToOstream(&output))
  {
    std::cerr << "Failed to write postscript for " << filename << "\n";
    exit(1);
  }
  output.put(static_cast<char>(ps.ByteSizeLong()));
}

/**
 * Create a file from a future version 19.99.
 */
void writeVersion1999()
{
  orc::proto::Metadata meta;
  orc::proto::Footer footer;
  footer.set_headerlength(3);
  footer.set_contentlength(3);
  orc::proto::Type *type = footer.add_types();
  type->set_kind(orc::proto::Type_Kind_STRUCT);
  footer.set_numberofrows(0);
  footer.set_rowindexstride(10000);
  orc::proto::ColumnStatistics *stats = footer.add_statistics();
  stats->set_numberofvalues(0);
  stats->set_hasnull(false);
  std::vector<std::uint32_t> version;
  version.push_back(19);
  version.push_back(99);
  writeCustomOrcFile("version1999.orc", meta, footer, version, 1);
}

int main(int argc, char **argv)
{
  // MemoryPool *pool = getDefaultPool();
  // assert(argc == 2);
  if (argc != 4)
  {
    exit(1);
  }
  std::string file_name = argv[1];
  std::string min_date = argv[2];
  std::string max_date = argv[3];
  ReaderOptions readerOpts;
  std::unique_ptr<Reader> reader =
      createReader(readLocalFile(file_name), readerOpts);
  RowReaderOptions rowReaderOpts;
  std::unique_ptr<SearchArgument> sarg =
      SearchArgumentFactory::newBuilder()
          ->startAnd()
          // .lessThan("l_quantity", PredicateDataType::LONG,
          //           Literal(static_cast<int64_t>(24L)))
          .between("l_shipdate", PredicateDataType::STRING,
                   Literal(min_date.c_str(), 10), Literal(max_date.c_str(), 10))
          .end()
          .build();
  rowReaderOpts.searchArgument(std::move(sarg));
  auto rowReader = reader->createRowReader(rowReaderOpts);
  auto readBatch = rowReader->createRowBatch(2000);
  // EXPECT_EQ(true, rowReader->next(*readBatch));
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
  }
  // std::cout << "total read:" << cnt << std::endl;
  // system("cat /proc/$PPID/io");
  return 0;
}
