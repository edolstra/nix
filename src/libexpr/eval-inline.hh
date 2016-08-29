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
    if (v.type == tThunk) {
        Value vTmp;
        v.thunk.expr->eval(*this, *v.thunk.env, vTmp);
        v = vTmp;
    }
    else if (v.type == tApp) {
        Value vTmp;
        callFunction(*v.app.left, *v.app.right, vTmp, noPos);
        v = vTmp;
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
