#include "command.hh"
#include "common-args.hh"
#include "common-opts.hh"
#include "shared.hh"
#include "store-api.hh"
#include "eval.hh"
#include "eval-inline.hh"
#include "thread-pool.hh"
#include "globals.hh"

using namespace nix;

struct CmdSearch : StoreCommand, MixDryRun
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

        std::function<void(Value *, std::string)> doExpr;

        struct State {
            std::unordered_set<Value *> done;
        };

        Sync<State> state_;

        doExpr = [&](Value * v, std::string attrPath) {
            //printMsg(lvlError, format("AT ‘%s’ %x") % attrPath % v);

            {
                auto state(state_.lock());
                if (state->done.count(v)) return;
                state->done.insert(v);
            }

            try {

                state.forceValue(*v);

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

                    if (attrPath != "") {
                        Bindings::iterator j = v->attrs->find(state.symbols.create("recurseForDerivations"));
                        if (j == v->attrs->end() || !state.forceBool(*j->value)) return;
                    }

                    std::vector<ThreadPool::work_t> work;

                    for (auto & i : *v->attrs)
                        work.emplace_back(std::bind(doExpr, i.value,
                                attrPath == "" ? (std::string) i.name : attrPath + "." + (std::string) i.name));

                    std::random_shuffle(work.begin(), work.end());

                    for (auto & i : work)
                        pool.enqueue(i);
                }

            } catch (AssertionError & e) {
            }
        };

        Expr * e = state.parseExprFromFile(resolveExprPath(lookupFileArg(state, "<nixpkgs>")));

        Value vRoot, vRoot2;
        state.eval(e, vRoot);

        state.autoCallFunction(*state.allocBindings(1), vRoot, vRoot2);

        pool.enqueue(std::bind(doExpr, &vRoot2, ""));

        pool.process();
    }
};

static RegisterCommand r1(make_ref<CmdSearch>());
