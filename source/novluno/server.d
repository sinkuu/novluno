module novluno.server;

import novluno.cache;
import novluno.config;
import novluno.node;
import novluno.status;

import optional;

import vibe.core.log;
import vibe.data.json;
import vibe.http.client;
import vibe.http.server;
import vibe.inet.path;
import vibe.stream.operations;
import vibe.web.web;

import std.algorithm;
import std.array;
import std.conv;
import std.random;
import std.range;
import std.string;
import std.typecons;
import std.utf;

final class ServerWebInterface
{
    enum responseContentType = "text/plain; charset=UTF-8";

    void index(HTTPServerResponse res)
    {
        res.writeBody("Novluno\n");
    }

    @path("/ping")
    void getPing(HTTPServerRequest req, HTTPServerResponse res)
    {
        res.writeBody("PONG\n" ~ req.peer);
    }

    @path("/node")
    void getNode(HTTPServerResponse res)
    {
        auto n = randomKnownNode();
        enforceHTTP(n !is null, HTTPStatus.serviceUnavailable);
        res.writeBody(n);
    }

    @path("/join/:node")
    void getJoin(HTTPServerRequest req, HTTPServerResponse res, string _node)
    {
        enforceHTTP(_node.length > 1, HTTPStatus.forbidden);

        if (_node[0] == ':') _node = req.peer ~ _node;

        auto node = Node(_node.replace("+", "/"));
        enforceHTTP(resolveHost(node.host).toAddressString() == req.peer,
            HTTPStatus.forbidden);

        // FIXME: don't accept join from localhost

        // TODO: if (g_status.linkedNodes.canFind(node)) 既に接続している場合、おそらく相手は再起動後?
        auto pong = node.ping();
        enforceHTTP(pong.success, HTTPStatus.forbidden);
        // TODO: enforceHTTP(pong.addr == req.addr, HTTPStatus.forbidden);

        g_status.linkedNodes.writer() ~= node;
        g_status.searchNodes.writer() ~= node;

        string n = randomKnownNode();
        if (n is null)
            res.writeBody("WELCOME\n");
        else
            res.writeBody("WELCOME\n" ~ n);
    }

    @path("/bye/:node")
    void getBye(HTTPServerRequest req, HTTPServerResponse res, string _node)
    {
        auto node = Node(_node.replace("+", "/"));
        enforceHTTP(resolveHost(node.host).toAddressString() == req.peer,
            HTTPStatus.forbidden);

        g_status.linkedNodes.writer().removeKey(node);

        res.writeBody("BYEBYE\n");
    }

    @path("/have/:filename")
    void getHave(HTTPServerResponse res, string _filename)
    {
        if (isValidThreadFileName(_filename) && g_cache.hasFile(_filename))
        {
            res.writeBody("YES");
        }
        else
        {
            res.writeBody("NO");
        }
    }

    @path("/get/:filename/:stamprange")
    void getGet(HTTPServerResponse res, string _filename, string _stamprange)
    {
        if (!isValidThreadFileName(_filename) || !g_cache.hasFile(_filename))
		{
			return;
		}

        res.contentType = responseContentType;

        auto writer = res.bodyWriter;
        foreach (rec; g_cache.getRecordStringsByRange(_filename,
            stampRangeTuple(_stamprange).expand))
        {
            writer.write(rec);
            writer.write("\n");
        }
    }

    @path("/get/:filename/:stamp/:id")
    void getGet(HTTPServerResponse res, string _filename, long _stamp, string _id)
    {
		auto head = RecordHead(_stamp, _id, _filename);
        if (!isValidThreadFileName(_filename) || !isValidRecordId(_id)
			|| !g_cache.hasFile(_filename) || !g_cache.hasRecord(head))
		{
			return;
		}

		res.writeBody(g_cache.getRecordString(head));
    }

    @path("/head/:filename/:stamprange")
    void getHead(HTTPServerResponse res, string _filename, string _stamprange)
    {
        //enforceHTTP(isValidThreadFileName(_filename), HTTPStatus.notFound);
        //enforceHTTP(g_cache.hasFile(_filename), HTTPStatus.notFound);
        if (!isValidThreadFileName(_filename) || !g_cache.hasFile(_filename))
		{
			return;
		}

        res.contentType = responseContentType;

        auto writer = res.bodyWriter;
        foreach (rec; g_cache.getRecordHeadsByRange(_filename,
                    stampRangeTuple(_stamprange).expand))
        {
            rec.toShingetsuHead!(s => writer.write(s))();
            writer.write("\n");
        }
    }

    @path("/update/:filename/:stamp/:id/:node")
    void getUpdate(HTTPServerRequest req, HTTPServerResponse res, string _filename,
        long _stamp, string _id, string _node)
    {
        import std.exception : ifThrown;
        import std.datetime : Clock;

        if (_stamp < (Clock.currTime().toUnixTime() - config.updateRangePast.total!"seconds") ||
            _stamp > (Clock.currTime().toUnixTime() + config.updateRangeFuture.total!"seconds"))
        {
            logDebug("server: received an update for a too old or future record");
            throw new HTTPStatusException(HTTPStatus.forbidden);
        }

        _node = _node.replace("+", "/");
        if (_node.byChar.startsWith(':'))
        {
            _node = req.peer ~ _node;
        }

        auto node = Node(_node);

        if (node.host != req.peer &&
                resolveHost(node.host).toAddressString().ifThrown("ERROR") != req.peer &&
                false /+ TODO !(g_cache.node.canFind(node)) +/)
        {
            // saku just ignores updates in this case
            return;
        }

        // TODO: deny/allow

        // TODO: disconnect with bad node
        // note: saku doesn't reject bad requests
        enforceHTTP(isValidThreadFileName(_filename) && isValidRecordId(_id), HTTPStatus.forbidden);

        res.writeBody("OK\n");

        import vibe.core.core : runTask;

        runTask(
        {
            string noderep = _node.replace("+", "/");
            Node holder = _node.startsWith(':') ? Node(req.peer ~ noderep) : Node(noderep);
            // TODO: cache
            enforceHTTP(holder.ping().success, HTTPStatus.forbidden);

            const head = RecordHead(_stamp, _id, _filename);
            g_cache.updateRecent(head);

            // TODO: implement sharing node per file

            if (g_cache.hasFile(_filename))
            {
                if (!g_cache.hasRecord(head))
                {
                    // TODO: if (spam) return; // don't relay

                    import optional.util : optSwitch;
                    holder.get(head).optSwitch!(
                        (Record r)
                        {
                            if (r.head == head) g_cache.addRecord(r);
                            else logDebug("got a mismatching record %s", r.toShingetsuRecord);
                        },
                        {});

                }

                holder = selfNode;
            }

            // relay
            foreach (n; g_status.linkedNodes.reader())
            {
                n.update(head, holder);
            }
        });
    }

    @path("/recent/:stamprange")
    void getRecent(HTTPServerResponse res, string _stamprange)
    {
        res.contentType = responseContentType;

        auto writer = res.bodyWriter;

        // saku treats empty range as "0-". Should Novluno do the same?
        foreach (r; g_cache.getRecentByRange(stampRangeTuple(_stamprange).expand))
        {
            r.toShingetsuHead!((scope string s) => writer.write(s))();
            writer.write("<>");
            writer.write(r.filename);
            writer.write("\n");
        }
    }

    private string randomKnownNode()
    {
        auto nodelist = chain(g_status.linkedNodes.reader()[], g_status.searchNodes.reader()[]);
        if (nodelist.length == 0) return null;
        return nodelist.randomSample(1).front.toString;
    }
}

private Tuple!(long, long) stampRangeTuple(string s) pure @safe
{
    // -end
    if (s[0] == '-')
    {
        return tuple(0L, s[1 .. $].to!long);
    }

    // start-
    if (s[$ - 1] == '-')
    {
        return tuple(s[0 .. $ - 1].to!long, long.max);
    }

    immutable idx = s.indexOf('-');

    // start-end
    if (idx != -1)
    {
        return tuple(s[0 .. idx].to!long, s[idx + 1 .. $].to!long);
    }

    auto t = s.to!long;
    return tuple(t, t);
}

pure @safe unittest
{
    assert(stampRangeTuple("1000") == tuple(1000, 1000));
    assert(stampRangeTuple("-1000") == tuple(0, 1000));
    assert(stampRangeTuple("1000-") == tuple(1000, long.max));
    assert(stampRangeTuple("1000-2000") == tuple(1000, 2000));
    assert(stampRangeTuple("0-") == tuple(0, long.max));
}

