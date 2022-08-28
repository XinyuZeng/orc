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

#include "ColumnWriter.hh"
#include "MemoryOutputStream.hh"
#include "RLE.hh"
#include "Stats.hh"
#include "orc/Common.hh"
#include "orc/Reader.hh"
#include "orc/Writer.hh"

#include <chrono>
#include <iostream>

using namespace orc;

const size_t DEFAULT_MEM_STREAM_SIZE = 10 * 1024 * 1024;  // 20MB
const size_t STRIPE_SIZE = 1 * 1024 * 1024;
#define BATCH_SIZE 1024

// int main()
int main(int argc, char* argv[]) {
  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " <file>" << std::endl;
    return 1;
  }
  std::string source_file = std::string(argv[1]);
  std::vector<int64_t> data;
  std::ifstream srcFile(source_file, std::ios::in);
  if (!srcFile) {
    std::cout << "error opening source file." << std::endl;
    return 0;
  }
  while (1) {
    int64_t next;
    srcFile >> next;
    if (srcFile.eof()) {
      break;
    }
    data.push_back(next);
  }
  srcFile.close();
  MemoryPool* pool = getDefaultPool();
  size_t num_buffers = data.size() / STRIPE_SIZE;
  std::vector<MemoryOutputStream*> memStreams;
  // MemoryOutputStream *memStreams[num_buffers];
  for (size_t i = 0; i < num_buffers; i++) {
    memStreams.push_back(new MemoryOutputStream(DEFAULT_MEM_STREAM_SIZE));
    // memStreams[i] = new MemoryOutputStream(DEFAULT_MEM_STREAM_SIZE);
  }
  // alignedBitPacking true when compression strategy is speed
  bool alignedBitPacking = true;
  // isSigned is false in string reader (verified)
  bool isSigned = false;
  size_t data_idx = 0;
  size_t encoded_size = 0;
  for (size_t i = 0; i < num_buffers; i++) {
    auto encoder =
        createRleEncoder(std::unique_ptr<BufferedOutputStream>(new BufferedOutputStream(
                             *pool, memStreams[i], 500 * 1024, 1024)),
                         isSigned, RleVersion_2, *pool, alignedBitPacking);
    for (; data_idx < data.size() && data_idx < STRIPE_SIZE * (i + 1); ++data_idx) {
      encoder->add(&data[data_idx], 1, nullptr);
    }
    encoder->flush();
    encoded_size += memStreams[i]->getLength();
    // for (size_t i = 0; i < data.size() / STRIPE_SIZE; i++)
    // {
    //   encoder->add(&data[i * STRIPE_SIZE], STRIPE_SIZE, nullptr);
    //   encoder->flush();
    // }
    // encoder->add(&data[(data.size() / STRIPE_SIZE) * STRIPE_SIZE], data.size() %
    // STRIPE_SIZE, nullptr); encoder->flush();
  }
  std::cout << "encoded size: " << encoded_size << std::endl;
  int64_t decode_time = 0;
  data_idx = 0;
  std::vector<int64_t> decoded_data;
  decoded_data.resize(data.size());
  int64_t* batch = decoded_data.data();
  for (size_t i = 0; i < num_buffers; ++i) {
    auto decoder = createRleDecoder(
        std::unique_ptr<SeekableArrayInputStream>(new SeekableArrayInputStream(
            memStreams[i]->getData(), memStreams[i]->getLength())),
        isSigned, RleVersion_2, *pool, nullptr);
    int64_t* start_tmp = batch;
    auto begin = std::chrono::high_resolution_clock::now();
    for (size_t j = 0; j < STRIPE_SIZE / BATCH_SIZE; j++) {
      decoder->next(batch, BATCH_SIZE, nullptr);
      batch += BATCH_SIZE;
    }
    decode_time += std::chrono::duration_cast<std::chrono::nanoseconds>(
                       std::chrono::high_resolution_clock::now() - begin)
                       .count();
    // decode_time += (static_cast<std::chrono::duration<double>>(
    //                     std::chrono::high_resolution_clock::now() - begin))
    //                    .count();
    for (size_t j = 0; j < STRIPE_SIZE; j++) {
      if (start_tmp[j] != data[data_idx]) {
        std::cout << "error: " << batch[j] << " != " << data[data_idx] << std::endl;
      }
      data_idx++;
    }
  }
  std::cout << "decode time (ns): " << decode_time << std::endl;
  std::cout << "# of encoded: " << data_idx << std::endl;
  std::cout << "num of short repeats: " << num_short_repeats
            << "func calls: " << num_short_repeats_func
            << "time: " << time_short_repeats_func << std::endl;
  std::cout << "num of direct: " << num_direct << "func calls: " << num_direct_func
            << "time: " << time_direct_func << std::endl;
  std::cout << "num of patched base: " << num_patched_base << "func calls"
            << num_patched_base_func << "time: " << time_patched_base_func << std::endl;
  std::cout << "num of delta: " << num_delta << "func calls: " << num_delta_func
            << "time: " << time_delta_func << std::endl;
  std::cout << "num of read first byte: " << num_read_first_byte << std::endl;
  return 0;
}
