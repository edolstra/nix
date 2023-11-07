#include "command.hh"
#include "shared.hh"
#include "store-api.hh"
#include "compression.hh"
#include "callback.hh"
#include "names.hh"

using namespace nix;

struct CmdAnalyseS3 : StoreCommand
{
    Path rootsDir;
    Path livePaths;
    Path summary;

    CmdAnalyseS3()
    {
        addFlag({
            .longName = "roots-dir",
            .description = "Directory containing binary cache roots.",
            .labels = {"path"},
            .handler = {&rootsDir},
        });

        addFlag({
            .longName = "live-paths",
            .description = "File to which the list of reachable paths will be written.",
            .labels = {"path"},
            .handler = {&livePaths},
        });

        addFlag({
            .longName = "summary",
            .description = "File to which a summary of release sizes will be written.",
            .labels = {"path"},
            .handler = {&summary},
        });
    }

    void run(ref<Store> store) override
    {
        settings.ttlPositiveNarInfoCache = 1000000000;
        settings.ttlNegativeNarInfoCache = 1000000000; // FIXME

        size_t max = 50;

        assert(rootsDir != "");
        assert(livePaths != "");
        assert(summary != "");

        printError("ROOTS %s", rootsDir);

        if (rootsDir.empty())
            throw Error("'--roots-dir' is required");

        auto rootsFiles =
            tokenizeString<std::vector<Path>>(runProgram("find", true, {rootsDir, "-type", "f"}));

        struct Version
        {
            std::string v;
            bool operator < (const Version & b) const
            {
                //printError("COMP %s %s", v, b.v);
                return compareVersions(v, b.v) == -1;
            }
        };

        auto toVersionVector = [](const std::vector<std::string> & v)
        {
            std::vector<Version> v2;
            for (auto & s : v)
                v2.emplace_back(Version(s));
            return v2;
        };

        std::stable_sort(rootsFiles.begin(), rootsFiles.end(),
            [&](const std::string & a, const std::string & b)
            {
                auto a2 = toVersionVector(tokenizeString<std::vector<std::string>>(a, "/-."));
                auto b2 = toVersionVector(tokenizeString<std::vector<std::string>>(b, "/-."));
                return a2 < b2;
                #if 0
                std::string a2{baseNameOf(dirOf(a))};
                std::string a3{baseNameOf(dirOf(dirOf(a)))};
                DrvName a4(a2);
                Version a5(a4.version);
                std::string b2{baseNameOf(dirOf(b))};
                std::string b3{baseNameOf(dirOf(dirOf(b)))};
                DrvName b4(b2);
                Version b5(b4.version);
                return std::tie(a3, a4.name, a5) < std::tie(b3, b4.name, b5);
                #endif
            });

        struct State
        {
            StorePathSet todo;
            StorePathSet handled;
            size_t pending = 0;
            size_t failed = 0;
            uint64_t totalNarSize = 0;
            uint64_t totalFileSize = 0;
            uint64_t totalNarInfoSize = 0;
            AutoCloseFD fdLivePaths;
            AutoCloseFD fdSummary;
        };

        Sync<State> _state;

        auto createFile = [](const Path & path)
        {
            auto fd = open(path.c_str(), O_WRONLY | O_TRUNC | O_CREAT | O_CLOEXEC, 0666);
            if (fd == -1) throw SysError("creating '%s'", path);
            return fd;
        };

        _state.lock()->fdLivePaths = createFile(livePaths);
        _state.lock()->fdSummary = createFile(summary);

        std::condition_variable wakeup;

        Activity act(*logger, actVerifyPaths);
        Activity act2(*logger, actFileTransfer);

        auto updateProgress = [&](State & state)
        {
            act.progress(
                state.handled.size() - state.pending,
                state.handled.size() + state.todo.size(),
                state.pending,
                state.failed);
            act2.progress(state.totalFileSize, state.totalFileSize);
        };

        auto enqueue = [&](State & state, const StorePath & path)
        {
            if (!state.handled.count(path))
                state.todo.insert(path);
        };

        auto startOne = [&]() -> bool
        {
            std::optional<StorePath> path;

            {
                auto state(_state.lock());
                if (state->todo.empty() || state->pending >= max) return false;
                path = *state->todo.begin();
                state->todo.erase(state->todo.begin());
                if (!state->handled.insert(*path).second)
                    return false;
                state->pending++;
                updateProgress(*state);
            }

            store->queryPathInfo(*path, {[&](std::future<ref<const ValidPathInfo>> result) {
                try {
                    auto info = result.get();
                    auto narInfo = info.dynamic_pointer_cast<const NarInfo>();
                    auto narInfoSize = narInfo ? narInfo->to_string(*store).size() : 0;
                    auto fileSize = narInfo ? narInfo->fileSize : 0;
                    writeFull(_state.lock()->fdLivePaths.get(), fmt("%s\t%d\t%d\t%d\n",
                        store->printStorePath(info->path),
                        info->narSize,
                        fileSize,
                        narInfoSize));
                    {
                        auto state(_state.lock());
                        state->totalNarSize += info->narSize;
                        state->totalFileSize += fileSize;
                        state->totalNarInfoSize += narInfoSize;
                        for (auto & p : info->references)
                            enqueue(*state, p);
                    }
                } catch (Error & e) {
                    logError(e.info());
                    _state.lock()->failed++;
                }
                auto state(_state.lock());
                assert(state->pending);
                state->pending--;
                if (!state->pending)
                    wakeup.notify_all();
                updateProgress(*state);
            }});

            return true;
        };

        uint64_t totalPaths = 0;
        uint64_t totalNarSize = 0;
        uint64_t totalFileSize = 0;
        uint64_t totalNarInfoSize = 0;

        for (auto & rootsFile : rootsFiles) {
            auto name = rootsFile.substr(rootsDir.size() + 1);

            auto data = decompress("xz", readFile(rootsFile));

            {
                auto roots = tokenizeString<std::vector<std::string>>(data);
                for (auto & p : roots)
                    enqueue(*_state.lock(), store->parseStorePath(p));
                writeFull(_state.lock()->fdSummary.get(), fmt("'%s': %d roots\n", name, roots.size()));
            }

            while (true) {
                while (startOne()) ;
                auto state(_state.lock());
                if (!state->pending && state->todo.empty()) break;
                state.wait(wakeup);
            }

            {
                auto state(_state.lock());
                writeFull(state->fdSummary.get(), fmt("'%s': +%d store paths, +%d GiB uncompressed size, +%d GiB compressed size, +%d GiB estimated .narinfo size\n",
                    name,
                    state->handled.size() - totalPaths,
                    (double) (state->totalNarSize - totalNarSize) / (1 << 30),
                    (double) (state->totalFileSize - totalFileSize) / (1 << 30),
                    (double) (state->totalNarInfoSize - totalNarInfoSize) / (1 << 30)));
                totalPaths = state->handled.size();
                totalNarSize = state->totalNarSize;
                totalFileSize = state->totalFileSize;
                totalNarInfoSize = state->totalNarInfoSize;
            }
        }

        {
            auto state(_state.lock());
            writeFull(state->fdSummary.get(), fmt("Done! Found %d store paths, %d GiB uncompressed size, %d GiB compressed size, %d GiB estimated .narinfo size\n",
                state->handled.size(),
                (double) state->totalNarSize / (1 << 30),
                (double) state->totalFileSize / (1 << 30),
                (double) state->totalNarInfoSize / (1 << 30)));
        }
    }
};

static auto rCmdAnalyseS3 = registerCommand2<CmdAnalyseS3>({"analyse-s3"});
