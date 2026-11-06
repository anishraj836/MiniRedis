#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace miniredis {

// Rewrites a write command so that replaying it later (from the AOF) has the
// same effect it has now. Relative expiries are the problem: "EXPIRE k 10"
// logged as-is would, on restart, give the key a fresh 10 seconds, so keys
// that expired long ago come back to life. They are converted to absolute
// unix-millisecond deadlines:
//
//   EXPIRE k s / PEXPIRE k ms / EXPIREAT k s  ->  PEXPIREAT k <deadline-ms>
//   SETEX k s v                               ->  SET k v PXAT <deadline-ms>
//   SET k v ... EX s | PX ms | EXAT s ...     ->  SET k v ... PXAT <deadline-ms> ...
//
// The rewritten command is executed instead of the original, so the client
// sees the same reply and the logged command matches what was applied.
// Returns std::nullopt when nothing needs to change (including malformed
// input, which is left alone so the original command reports the error).
std::optional<std::vector<std::string>> normalize_for_propagation(const std::vector<std::string>& tokens,
                                                                  int64_t now_ms);

} // namespace miniredis
