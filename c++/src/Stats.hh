#ifndef ORC_STATS_HH
#define ORC_STATS_HH

#include <atomic>
#include <chrono>


namespace orc {
extern uint64_t num_read;
extern int64_t time_read;
extern int64_t time_meta;
extern int64_t time_levels;
extern int64_t time_decode; // only record time spent on decoding
}  // namespace orc

#endif
