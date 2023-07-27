#include "Stats.hh"
namespace orc {
uint64_t num_read = 0;
int64_t time_read = 0;
int64_t time_meta = 0;
int64_t time_startNextStripe = 0;
int64_t time_buildReader = 0;
int64_t time_levels = 0;
int64_t time_decode = 0;
uint64_t n_short_repeats = 0;
uint64_t n_direct = 0;
uint64_t n_patched = 0;
uint64_t n_delta = 0;
uint64_t n_short_repeats_seq = 0;
uint64_t n_direct_seq = 0;
uint64_t n_patched_seq = 0;
uint64_t n_delta_seq = 0;
}  // namespace orc