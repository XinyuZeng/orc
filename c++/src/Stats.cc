#include "Stats.hh"
#include <chrono>
namespace orc {
uint64_t num_short_repeats = 0;
uint64_t num_direct = 0;
uint64_t num_patched_base = 0;
uint64_t num_delta = 0;
uint64_t num_short_repeats_func = 0;
uint64_t num_direct_func = 0;
uint64_t num_patched_base_func = 0;
uint64_t num_delta_func = 0;
int64_t time_short_repeats_func = 0;
int64_t time_direct_func = 0;
int64_t time_patched_base_func = 0;
int64_t time_delta_func = 0;
uint32_t num_read_first_byte = 0;
}  // namespace orc
