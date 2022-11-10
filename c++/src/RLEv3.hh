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

#ifndef ORC_RLEV3_HH
#define ORC_RLEV3_HH

#include "Adaptor.hh"
#include "orc/Exceptions.hh"
#include "RLE.hh"
#include "arrow/util/rle_encoding.h"

#include <vector>

namespace orc {


class RleEncoderV3 : public RleEncoder {
public:
    RleEncoderV3(std::unique_ptr<BufferedOutputStream> outStream, bool hasSigned, bool alignBitPacking = true);

    ~RleEncoderV3() override {
      delete [] literals;
    }
    /**
     * Flushing underlying BufferedOutputStream
     */
    uint64_t flush() override;

    void write(int64_t val) override;

private:
  // arrow::util::RleEncoder rleEncoder;
};

class RleDecoderV2 : public RleDecoder {
public:
  RleDecoderV2(std::unique_ptr<SeekableInputStream> input,
               bool isSigned, MemoryPool& pool);

  /**
  * Seek to a particular spot.
  */
  void seek(PositionProvider&) override;

  /**
  * Seek over a given number of values.
  */
  void skip(uint64_t numValues) override;

  /**
  * Read a number of values into the batch.
  */
  void next(int64_t* data, uint64_t numValues,
            const char* notNull) override;

private:

  /**
   * Decode the next gap and patch from 'unpackedPatch' and update the index on it.
   * Used by PATCHED_BASE.
   *
   * @param patchBitSize  bit size of the patch value
   * @param patchMask     mask for the patch value
   * @param resGap        result of gap
   * @param resPatch      result of patch
   * @param patchIdx      current index in the 'unpackedPatch' buffer
   */
  void adjustGapAndPatch(uint32_t patchBitSize, int64_t patchMask,
                         int64_t* resGap, int64_t* resPatch, uint64_t* patchIdx);

  void resetReadLongs() {
    bitsLeft = 0;
    curByte = 0;
  }

  void resetRun() {
    resetReadLongs();
  }

  unsigned char readByte();

  int64_t readLongBE(uint64_t bsz);
  int64_t readVslong();
  uint64_t readVulong();
  void readLongs(int64_t *data, uint64_t offset, uint64_t len, uint64_t fbs);
  void plainUnpackLongs(int64_t *data, uint64_t offset, uint64_t len, uint64_t fbs);

  void unrolledUnpack4(int64_t *data, uint64_t offset, uint64_t len);
  void unrolledUnpack8(int64_t *data, uint64_t offset, uint64_t len);
  void unrolledUnpack16(int64_t *data, uint64_t offset, uint64_t len);
  void unrolledUnpack24(int64_t *data, uint64_t offset, uint64_t len);
  void unrolledUnpack32(int64_t *data, uint64_t offset, uint64_t len);
  void unrolledUnpack40(int64_t *data, uint64_t offset, uint64_t len);
  void unrolledUnpack48(int64_t *data, uint64_t offset, uint64_t len);
  void unrolledUnpack56(int64_t *data, uint64_t offset, uint64_t len);
  void unrolledUnpack64(int64_t *data, uint64_t offset, uint64_t len);

  uint64_t nextShortRepeats(int64_t* data, uint64_t offset, uint64_t numValues,
                            const char* notNull);
  uint64_t nextDirect(int64_t* data, uint64_t offset, uint64_t numValues,
                      const char* notNull);
  uint64_t nextPatched(int64_t* data, uint64_t offset, uint64_t numValues,
                       const char* notNull);
  uint64_t nextDelta(int64_t* data, uint64_t offset, uint64_t numValues,
                     const char* notNull);

  uint64_t copyDataFromBuffer(int64_t* data, uint64_t offset, uint64_t numValues,
                              const char* notNull);

  const std::unique_ptr<SeekableInputStream> inputStream;
  const bool isSigned;

  unsigned char firstByte;
  uint64_t runLength; // Length of the current run
  uint64_t runRead; // Number of returned values of the current run
  const char *bufferStart;
  const char *bufferEnd;
  uint32_t bitsLeft; // Used by readLongs when bitSize < 8
  uint32_t curByte; // Used by anything that uses readLongs
  DataBuffer<int64_t> unpackedPatch; // Used by PATCHED_BASE
  DataBuffer<int64_t> literals; // Values of the current run
};
}  // namespace orc

#endif  // ORC_RLEV3_HH
