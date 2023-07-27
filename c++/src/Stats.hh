#ifndef ORC_STATS_HH
#define ORC_STATS_HH

#include <atomic>
#include <chrono>


namespace orc {
extern uint64_t num_read;
extern int64_t time_read;
extern int64_t time_meta;
extern int64_t time_startNextStripe;
extern int64_t time_buildReader;
extern int64_t time_levels;
extern int64_t time_decode; // only record time spent on decoding
extern uint64_t n_short_repeats; 
extern uint64_t n_direct; 
extern uint64_t n_patched; 
extern uint64_t n_delta; 
extern uint64_t n_short_repeats_seq;
extern uint64_t n_direct_seq;
extern uint64_t n_patched_seq;
extern uint64_t n_delta_seq;
}  // namespace orc

#endif
