#include "storage/memory_tracker.hpp"
#include <iomanip>
#include <sstream>

namespace miniredis {

std::string MemoryTracker::format_memory_human() const {
    double bytes = static_cast<double>(get_used_memory());
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2);

    if (bytes < 1024.0) {
        oss << bytes << " B";
    } else if (bytes < 1024.0 * 1024.0) {
        oss << (bytes / 1024.0) << " KB";
    } else if (bytes < 1024.0 * 1024.0 * 1024.0) {
        oss << (bytes / (1024.0 * 1024.0)) << " MB";
    } else {
        oss << (bytes / (1024.0 * 1024.0 * 1024.0)) << " GB";
    }
    return oss.str();
}

} // namespace miniredis
