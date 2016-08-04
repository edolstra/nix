#pragma once

#include "eval.hh"

#define LocalNoInline(f) static f __attribute__((noinline)); f
#define LocalNoInlineNoReturn(f) static f __attribute__((noinline, noreturn)); f

namespace nix {

LocalNoInlineNoReturn(void throwEvalError(const char * s, const Pos & pos))
{
    throw EvalError(format(s) % pos);
}

LocalNoInlineNoReturn(void throwTypeError(const char * s, const Value & v))
{
    throw TypeError(format(s) % showType(v));
}


LocalNoInlineNoReturn(void throwTypeError(const char * s, const Value & v, const Pos & pos))
{
    throw TypeError(format(s) % showType(v) % pos);
}


void EvalState::forceValue(Value & v, const Pos & pos)
{
 restart:
    if (v.type == tThunk) {
        ValueType t = tThunk;
        if (!v.type.compare_exchange_strong(t, tBlackhole)) {
            //printMsg(lvlError, format("RESTART %d") % t);
            goto restart;
        }
        try {
            Value vTmp;
            v.thunk.expr->eval(*this, *v.thunk.env, vTmp);
            assert(v.type == tBlackhole);
            v = vTmp;
        } catch (...) {
            ValueType t2 = tBlackhole;
            if (!v.type.compare_exchange_strong(t2, tThunk)) {
                abort();
            }
            throw;
        }
        assert(v.type != tBlackhole && v.type != tThunk);
    }
    else if (v.type == tApp) {
        ValueType t = tApp;
        if (!v.type.compare_exchange_strong(t, tBlackhole)) {
            goto restart;
        }
        try {
            Value vTmp;
            callFunction(*v.app.left, *v.app.right, vTmp, noPos);
            assert(v.type == tBlackhole);
            v = vTmp;
        } catch (...) {
            ValueType t2 = tBlackhole;
            if (!v.type.compare_exchange_strong(t2, tApp)) {
                abort();
            }
            throw;
        }
        //assert(v.type != tBlackhole && v.type != tApp);
    }
    else if (v.type == tBlackhole) {
        //throwEvalError("infinite recursion encountered, at %1%", pos);
        while (v.type == tBlackhole)
            checkInterrupt();
        if (v.type == tThunk) goto restart;
    }
}


inline void EvalState::forceAttrs(Value & v)
{
    forceValue(v);
    if (v.type != tAttrs)
        throwTypeError("value is %1% while a set was expected", v);
}


inline void EvalState::forceAttrs(Value & v, const Pos & pos)
{
    forceValue(v);
    if (v.type != tAttrs)
        throwTypeError("value is %1% while a set was expected, at %2%", v, pos);
}


inline void EvalState::forceList(Value & v)
{
    forceValue(v);
    if (!v.isList())
        throwTypeError("value is %1% while a list was expected", v);
}


inline void EvalState::forceList(Value & v, const Pos & pos)
{
    forceValue(v);
    if (!v.isList())
        throwTypeError("value is %1% while a list was expected, at %2%", v, pos);
}

}
