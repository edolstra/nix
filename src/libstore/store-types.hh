#pragma once

#include <string>
#include <optional>

namespace nix {

struct Sink;
struct Source;

enum RepairFlag : bool { NoRepair = false, Repair = true };

struct StoreUser
{
    // FIXME: variant of user or group.
    std::string userName;

    bool operator < (const StoreUser & other) const
    {
        return userName < other.userName;
    }
};

typedef std::optional<StoreUser> Owner;

}
