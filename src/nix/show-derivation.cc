// FIXME: integrate this with nix path-info?

#include "command.hh"
#include "common-args.hh"
#include "store-api.hh"
#include "archive.hh"
#include "json.hh"
#include "derivations.hh"

using namespace nix;

struct CmdShowDerivation : InstallablesCommand
{
    bool recursive = false;

    CmdShowDerivation()
    {
        mkFlag()
            .longName("recursive")
            .shortName('r')
            .description("include the dependencies of the specified derivations")
            .set(&recursive, true);
    }

    std::string name() override
    {
        return "show-derivation";
    }

    std::string description() override
    {
        return "show the contents of a store derivation";
    }

    Examples examples() override
    {
        return {
            Example{
                "To show the store derivation that results from evaluating the Hello package:",
                "nix show-derivation nixpkgs.hello"
            },
            Example{
                "To show the full derivation graph (if available) that produced your NixOS system:",
                "nix show-derivation -r /run/current-system"
            },
        };
    }

    void run(ref<Store> store) override
    {
        auto drvPaths = toDerivations(store, installables, true);

        if (recursive) {
            PathSet closure;
            store->computeFSClosure(drvPaths, closure);
            drvPaths = closure;
        }

        {

        JSONObject jsonRoot(std::cout, true);

        for (auto & drvPath : drvPaths) {
            if (!isDerivation(drvPath)) continue;
            auto drv = readDerivation(drvPath);
            auto drvObj(jsonRoot.object(drvPath));
            drv.toJSON(drvObj);
        }

        }

        std::cout << "\n";
    }
};

static RegisterCommand r1(make_ref<CmdShowDerivation>());
