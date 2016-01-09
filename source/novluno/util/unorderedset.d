module novluno.util.unorderedset;

import std.range.primitives;
import std.traits;
import std.typetuple;

struct UnorderedSet(T)
{
    alias Dummy = const(void[0]);
    private Dummy[T] _aa;

    alias isImplicitlyConvertibleToT(U) = isImplicitlyConvertible!(U, T);

    size_t removeKey(U...)(auto ref U elems) if (allSatisfy!(isImplicitlyConvertibleToT, U))
    {
        size_t removed;

        foreach (const ref e; elems)
        {
            if (_aa.remove(e)) removed++;
        }

        return removed;
    }

    ref UnorderedSet opOpAssign(string op : "~")(auto ref T a)
    {
        (cast(void[0][T])_aa)[a] = Dummy.init;
        return this;
    }

    ref UnorderedSet opOpAssign(string op : "~")(in UnorderedSet uset)
    {
        foreach (a; uset[])
            (cast(void[0][T])_aa)[a] = Dummy.init;
        return this;
    }

    ref UnorderedSet opOpAssign(string op : "~", R)(R range) if (isInputRange!R && is(ElementType!R : T))
    {
        foreach (elem; range) (cast(void[0][T])_aa)[elem] = Dummy.init;
        return this;
    }

    UnorderedSet opBinary(string op : "~")(in T a) const
    {
        auto ret = dup();
        ret ~= a;
        return ret;
    }

    UnorderedSet opBinary(string op : "~")(in UnorderedSet uset) const
    {
        auto ret = dup();
        foreach (elem; uset[]) ret ~= elem;
        return ret;
    }

    UnorderedSet opBinary(string op : "~", R)(R range) if (isInputRange!R && is(ElementType!R : T))
    {
        auto ret = dup();
        foreach (elem; range) ret ~= elem;
        return ret;
    }

    auto opSlice() const
    {
        return _aa.keys;
    }

    bool opBinaryRight(string op : "in")(auto ref T a) const
    {
        return (a in _aa) !is null;
    }

    @property size_t length() const
    {
        return _aa.length;
    }

    UnorderedSet dup() const
    {
        return UnorderedSet(_aa.dup);
    }
}

unittest
{
    UnorderedSet!int set, set2;
    assert(1 !in set);
    set ~= 1;
    assert(1 in set);
    set ~= set;
    set ~= [2, 3];
    set2 ~= [3,4];
    const set3 = set2;
    assert(4 in (set ~ set3));
}
