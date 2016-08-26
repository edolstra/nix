#include "command.hh"
#include "common-args.hh"
#include "common-opts.hh"
#include "shared.hh"
#include "store-api.hh"
#include "eval.hh"
#include "eval-inline.hh"
#include "thread-pool.hh"
#include "globals.hh"
#include "installables.hh"

using namespace nix;

struct CmdSearch : StoreCommand, MixInstallables
{
    CmdSearch()
    {
    }

    std::string name() override
    {
        return "search";
    }

    std::string description() override
    {
        return "query available packages";
    }

    void run(ref<Store> store) override
    {
        settings.readOnlyMode = true;

        EvalState state({}, store);

        ThreadPool pool(std::stoi(getEnv("CORES", "1")));

        std::function<void(Value *, std::string, bool)> doExpr;

        struct State {
            std::unordered_set<Value *> done;
        };

        Sync<State> state_;

        doExpr = [&](Value * v, std::string attrPath, bool toplevel) {
            //printMsg(lvlError, format("AT ‘%s’ %x") % attrPath % v);

            {
                auto state(state_.lock());
                if (state->done.count(v)) return;
                state->done.insert(v);
            }

            try {

                state.forceValue(*v);

                if (v->type == tLambda && toplevel) {
                    Value * v2 = state.allocValue();
                    state.autoCallFunction(*state.allocBindings(1), *v, *v2);
                    v = v2;
                    state.forceValue(*v);
                }

                if (state.isDerivation(*v)) {

                    Bindings::iterator j = v->attrs->find(state.sName);
                    assert(j != v->attrs->end());
                    auto name = state.forceStringNoCtx(*j->value);

                    j = v->attrs->find(state.sDrvPath);
                    assert(j != v->attrs->end());
                    auto drvPath = state.forceString(*j->value);

                    std::cout << (format("%s %s %s\n") % attrPath % name % drvPath).str();
                }

                else if (v->type == tAttrs) {

                    if (!toplevel) {
                        auto attrs = v->attrs;
                        Bindings::iterator j = attrs->find(state.symbols.create("recurseForDerivations"));
                        if (j == attrs->end() || !state.forceBool(*j->value, *j->pos)) return;
                    }

                    Bindings::iterator j = v->attrs->find(state.symbols.create("_toplevel"));
                    bool toplevel2 = j != v->attrs->end() && state.forceBool(*j->value, *j->pos);

                    for (auto & i : *v->attrs) {
                        pool.enqueue(std::bind(doExpr, i.value,
                                attrPath == "" ? (std::string) i.name : attrPath + "." + (std::string) i.name, toplevel2));
                    }
                }

            } catch (AssertionError & e) {
            }
        };

        pool.enqueue(std::bind(doExpr, buildSourceExpr(state), "", true));

        pool.process();
    }
};

static RegisterCommand r1(make_ref<CmdSearch>());
