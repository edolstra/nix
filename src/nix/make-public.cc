#include "command.hh"
#include "store-api.hh"
#include "references.hh"
#include "common-args.hh"

using namespace nix;

struct CmdMakePublic : StorePathsCommand, MixJSON
{
    CmdMakePublic()
    {
        realiseMode = Realise::Outputs;
    }

    std::string description() override
    {
        return "make a store path world-readable";
    }

    std::string doc() override
    {
        return
          #include "make-public.md"
          ;
    }

    void run(ref<Store> store, StorePaths && storePaths) override
    {
        for (auto & path : storePaths)
            store->grantAccess(path, {});
    }
};

static auto rCmdMakePublic = registerCommand2<CmdMakePublic>({"store", "make-public"});
