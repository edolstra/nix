#pragma once
///@file

#include "nix/util/types.hh"
#include "nix/util/hash.hh"

#include <variant>

#include <nlohmann/json_fwd.hpp>

#include <future>
#include <optional>

namespace nix::fetchers {

using Attr = std::variant<std::string, uint64_t, Explicit<bool>>;

struct LazyAttr
{
    std::shared_future<Attr> attr;

    LazyAttr(Attr attr);
    LazyAttr(std::future<Attr> attr)
        : attr(std::move(attr)) {};
    LazyAttr(std::string s)
        : LazyAttr(Attr{std::move(s)}) {};
    LazyAttr(const char * s)
        : LazyAttr(Attr{std::string(s)}) {};
    LazyAttr(uint64_t n)
        : LazyAttr(Attr{n}) {};
    LazyAttr(Explicit<bool> b)
        : LazyAttr(Attr{b}) {};

    operator const Attr &() const
    {
        return attr.get();
    }

    const Attr & operator()() const
    {
        return attr.get();
    }

    bool operator==(const LazyAttr & other) const
    {
        return (*this)() == other();
    }

    bool operator!=(const LazyAttr & other) const
    {
        return (*this)() != other();
    }

    bool operator<(const LazyAttr & other) const
    {
        return (*this)() < other();
    }
};

/**
 * An `Attrs` can be thought of a JSON object restricted or simplified
 * to be "flat", not containing any subcontainers (arrays or objects)
 * and also not containing any `null`s.
 */
using Attrs = std::map<std::string, LazyAttr>;

Attrs jsonToAttrs(const nlohmann::json & json);

nlohmann::json attrsToJSON(const Attrs & attrs);

std::optional<std::string> maybeGetStrAttr(const Attrs & attrs, const std::string & name);

std::string getStrAttr(const Attrs & attrs, const std::string & name);

std::optional<uint64_t> maybeGetIntAttr(const Attrs & attrs, const std::string & name);

uint64_t getIntAttr(const Attrs & attrs, const std::string & name);

std::optional<bool> maybeGetBoolAttr(const Attrs & attrs, const std::string & name);

bool getBoolAttr(const Attrs & attrs, const std::string & name);

StringMap attrsToQuery(const Attrs & attrs);

Hash getRevAttr(const Attrs & attrs, const std::string & name);

} // namespace nix::fetchers
