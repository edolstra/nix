#include "eval.hh"
#include "util.hh"
#include "fs-input-accessor.hh"

namespace nix {

SourcePath EvalState::rootPath(const Path & path)
{
    return {rootFS, CanonPath(path)};
}

void EvalState::registerAccessor(ref<InputAccessor> accessor)
{
    inputAccessors.emplace(accessor->number, accessor);
}

std::string EvalState::encodePath(const SourcePath & path)
{
    /* For backward compatibility, return paths in the root FS
       normally. Encoding any other path is not very reproducible (due
       to /nix/store/virtual000...<N>) and we should deprecate it
       eventually. So print a warning about use of an encoded path in
       decodePath(). */
    return path.accessor == rootFS
        ? path.path.abs()
        : fmt("%s%08x-source%s", virtualPathMarker, path.accessor->number, path.path.absOrEmpty());
}

SourcePath EvalState::decodePath(std::string_view s, PosIdx pos, std::string_view errorCtx)
{
    if (!hasPrefix(s, "/"))
        error("string '%1%' doesn't represent an absolute path", s).withTrace(pos, errorCtx).debugThrow<EvalError>();

    if (hasPrefix(s, virtualPathMarker)) {
        auto fail = [this, s, pos, errorCtx]() {
            error("cannot decode virtual path '%1'", s).withTrace(pos, errorCtx).debugThrow<Error>();
        };

        s = s.substr(virtualPathMarker.size());

        try {
            auto slash = s.find('/');
            size_t number = std::stoi(std::string(s.substr(0, slash)), nullptr, 16);
            s = slash == s.npos ? "" : s.substr(slash);

            auto accessor = inputAccessors.find(number);
            if (accessor == inputAccessors.end()) fail();

            SourcePath path {accessor->second, CanonPath(s)};

            warn("applying 'toString' to path '%s' and then accessing it is deprecated, at %s", path, positions[pos]);

            return path;
        } catch (std::invalid_argument & e) {
            fail();
            abort();
        }
    } else
        return {rootFS, CanonPath(s)};
}

std::string EvalState::decodePaths(std::string_view s, PosIdx pos, std::string_view errorCtx)
{
    std::string res;

    size_t sPos = 0;

    while (true) {
        auto m = s.find(virtualPathMarker, sPos);
        if (m == s.npos) {
            res.append(s.substr(sPos));
            return res;
        }

        res.append(s.substr(sPos, m - sPos));

        auto end = s.find_first_of(" \n\r\t'\"’:", m);
        if (end == s.npos) end = s.size();

        try {
            auto path = decodePath(s.substr(m, end - m), pos, errorCtx);
            res.append(path.to_string());
        } catch (...) {
            throw;
            res.append(s.substr(sPos, end - m));
        }

        sPos = end;
    }
}

}
