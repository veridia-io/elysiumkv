#include "elysiumkv/status.hpp"

namespace elysiumkv {

std::string_view status_name(Status s) noexcept {
    switch (s) {
        case Status::Ok:       return "ok";
        case Status::NotFound: return "not_found";
        case Status::Corrupt:  return "corrupt";
        case Status::Unusable: return "unusable";
        case Status::Fenced:   return "fenced";
        case Status::Config:   return "config";
        case Status::Io:       return "io";
        case Status::Stalled:  return "stalled";
    }
    return "unknown";
}

}  // namespace elysiumkv
