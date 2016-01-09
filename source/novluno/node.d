module novluno.node;

import novluno.config;
import novluno.cache;

import optional;
import vibe.core.log;
import vibe.core.net;
import vibe.http.client;
import vibe.inet.url;
import vibe.stream.operations;
import vibe.core.stream : nullSink;

import std.algorithm;
import std.array;
import std.conv;
import std.exception;
import std.string;
import std.typecons;
import std.regex;

immutable Node selfNode;
immutable PathEntry selfPath;

shared static this()
{
    HTTPClient.setUserAgentString(config.serverString);

    version (unittest)
    {
        logInfo("Setting dummy selfNode");
        selfNode = Node("127.0.0.1", config.port, config.serverPath);
        selfPath = PathEntry(selfNode.toString().replace("/", "+"));
        // Just make sure the code below compiles
        if ((() => true)())
            return;
    }

    if (config.host.empty)
    {
        // TODO: compare results from multiple nodes?
        auto pings = config.initialNodes
            .map!(n => Node(n).ping())
            .cache
            .find!(t => t.success);

        debug
        {
            //if (pings.empty)
            //{
            //    selfNode = Node("127.0.0.1", config.port, config.serverPath);
            //}
            //else
            {
                enforce(!pings.empty, "No pong returned from initial nodes.");
                selfNode = Node(pings.front.addr, config.port, config.serverPath);
            }

        }
        else
        {
            enforce(!pings.empty, "No pong returned from initial nodes.");
            selfNode = Node(pings.front.addr, config.port, config.serverPath);
        }
    }
    else
    {
        selfNode = Node(config.host, config.port, config.serverPath);
    }

    selfPath = PathEntry(selfNode.toString().replace("/", "+"));

    logInfo("initialized self node: %s", selfNode.toString());
}

struct Node
{
    private
    {
        string _host;
        ushort _port;
        string _path;

        enum ipv4re = ctRegex!(`^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}` ~
                `(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$`);
        // TODO: ipv6
        enum hostre = ctRegex!(`^(?:[0-9a-z]([-0-9a-z]{0,61}[0-9a-z])?\.)+[a-z]+$`, "i");
    }

    this(string host, ushort port, string path) /+pure nothrow+/ @safe
    {
        _host = host;
        _port = port;
        _path = path;

        enforce(_host.match(ipv4re) || _host.match(hostre), "Invalid host for node name");
    }

    this(string name) /+pure+/ @safe
    {
        import std.string : indexOf;

        auto idx = name.indexOf(':');
        enforce(idx != -1, "Invalid node name");
        _host = name[0 .. idx];
		debug
		{
			enforce(_host.match(ipv4re) || _host.match(hostre) || _host == "localhost",
					"Invalid host for node name: " ~ _host);
		}
		else
		{
			enforce(_host.match(ipv4re) || _host.match(hostre),
					"Invalid host for node name: " ~ _host);
		}

        name = name[idx + 1 .. $];

        idx = name.indexOf('/');
        enforce(idx != -1, "Invalid node name");
        _port = name[0 .. idx].to!ushort;

        _path = name[idx .. $];
    }

    @property string host() const @safe pure nothrow @nogc
    {
        return _host;
    }

    @property ushort port() const @safe pure nothrow @nogc
    {
        return _port;
    }

    @property string path() const @safe pure nothrow @nogc
    {
        return _path;
    }

    @property string toString() const @safe pure nothrow
    {
        return host ~ ":" ~ port.to!string ~ path;
    }

    @property URL url() const
    {
        // TODO: cache?
        return URL("http://" ~ toString());
    }

    Tuple!(bool, "success", string, "addr") ping() const nothrow
    {
        //TODO: ping cache

        // TODO: timeout
        foreach (i; 0 .. config.pingRetry + 1)
        {
            try
            {
                bool success;
                string addr;

                auto pingURL = this.url ~ Path("ping");
                logInfo("ping: %s", pingURL);
                requestHTTP(pingURL,
                    (scope req) { },
                    (scope res)
                    {
                        success = res.statusCode == HTTPStatus.OK;
                        logDebug("ping: statusCode=%s", res.statusCode.to!string);
                        if (!success) return;

                        auto stream = res.bodyReader;
                        success &= stream.skipBytes(cast(ubyte[])"PONG\n");
                        if (!success) return;

                        addr = stream.readAllUTF8().chomp;
                    });
                if (success)
                    return typeof(return)(true, addr);
            }
            catch (Exception e)
            {
                logInfo("ping failed: %s", e);
            }
        }

        return typeof(return)(false, null);
    }

    Tuple!(bool, "success", Optional!Node, "node") join() const nothrow
    {
        import std.array : replace;

        foreach (i; 0 .. config.joinRetry + 1)
        {
            try
            {
                bool success;
                Optional!Node returned;

                auto joinURL = this.url ~ PathEntry("join") ~ selfPath;
                logInfo("join: %s", joinURL);
                // TODO: timeout
                requestHTTP(joinURL,
                    (scope req) { },
                    (scope res)
                    {
                        success = res.statusCode == HTTPStatus.OK;
                        logDebug("join: statusCode=%s", res.statusCode.to!string);
                        if (!success) return;

                        auto stream = res.bodyReader;
                        try
                        {
                            if (stream.empty)
                            {
                                logDebug("join: empty");
                                return;
                            }

                            // Note: readLine fails if there isn't '\n'
                            success &= stream.skipBytes(cast(ubyte[])"WELCOME\n");
                            logDebug("join: success=%s", success.to!string);

                            if (success && !stream.empty)
                            {
                                auto name = stream.readAllUTF8().chomp;
                                returned = Node(name);
                            }
                        }
                        catch (Exception e)
                        {
                            logDebug("%s", e);
                            success = false;
                        }
                    });

                if (success)
                    return typeof(return)(success, returned);
            }
            catch (Exception e)
            {
                logInfo("join failed: %s", e);
            }
        }

        return typeof(return)(false, Optional!Node());
    }

    bool bye() const nothrow
    {
        try
        {
            auto byeURL = this.url ~ PathEntry("bye") ~ selfPath;
            logInfo("bye: %s", byeURL);
            requestHTTP(byeURL,
                (scope req) { },
                (scope res) { });

            return true;
        }
        catch (Exception e)
        {
            logInfo("bye failed: %s", e);
            return false;
        }
    }

    Optional!Record get(RecordHead head) const nothrow
    {
        try
        {
            Optional!Record record;

            auto getURL = this.url ~ PathEntry("get")
                ~ PathEntry(head.filename) ~ PathEntry(head.stamp.text) ~ PathEntry(head.id);
            logInfo("get: %s", getURL);
            requestHTTP(getURL,
                (scope req) { },
                (scope res) {
                    record = parseShingetsuRecord(res.bodyReader.readAllUTF8().chomp,
                        head.filename);
                });

            return record;
        }
        catch (Exception e)
        {
            logDebug("get failed: %s", e);
            return Optional!Record();
        }
    }

    Optional!(Record[]) get(string filename, Optional!long beginTime,
		Optional!long endTime = Optional!long()) const nothrow
    {
        try
        {
            Optional!Record record;

            auto getURL = this.url ~ PathEntry("get")
                ~ PathEntry(filename) ~ PathEntry(stampRangeString(beginTime, endTime));
            logInfo("get: %s", getURL);
            auto records = appender!(Record[])();
            requestHTTP(getURL,
                (scope req) { },
                (scope res) {
                    auto stream = res.bodyReader;
                    while (!stream.empty)
                    {
                        auto line = (cast(string)stream.readLine(size_t.max, "\n")).chomp;
                        logDebug("get: %s", line);
                        records ~= parseShingetsuRecord(line, filename);
                    }
                });

            return Optional!(Record[])(records.data);
        }
        catch (Exception e)
        {
            logDebug("get failed: %s", e);
            return Optional!(Record[])();
        }
    }

	private static string stampRangeString(Optional!long beginTime, Optional!long endTime)
		pure nothrow @safe
	{
		string res;
		if (!beginTime.empty && beginTime.get > 0) res ~= beginTime.get.text;
		res ~= '-';
		if (!endTime.empty && endTime.get < long.max) res ~= endTime.get.text;
		return res;
	}

    RecordHead[] recent(Optional!long beginTime, Optional!long endTime) const nothrow
    {
        try
        {
            auto records = appender!(RecordHead[])();

            auto talkURL = this.url ~ PathEntry("recent")
                ~ PathEntry(stampRangeString(beginTime, endTime));
            logInfo("recent: %s", talkURL);
            requestHTTP(talkURL,
                (scope req) { },
                (scope res) {
                    auto stream = res.bodyReader;
                    while (!stream.empty)
                    {
                        auto line = (cast(string)stream.readLine(size_t.max, "\n")).chomp;
                        logDebug("recent: %s", line);
                        auto rf = parseRecentRecord(line);
                        records ~= rf.recordHead;
                        // TODO: tags
                    }
                });

            return records.data;
        }
        catch (Exception e)
        {
            logDebug("recent failed: %s", e);
            return [];
        }
    }

    bool update(RecordHead head, Node holder) const nothrow
    {
        try
        {
            auto talkURL = this.url ~ PathEntry("update")
                ~ PathEntry(head.filename) ~ PathEntry(head.stamp.text) ~ PathEntry(head.id)
                ~ PathEntry(holder.toString().replace("/", "+"));
            logInfo("update: %s", talkURL);
            requestHTTP(talkURL,
                (scope req) { },
                (scope res) { });

            return true;
        }
        catch (Exception e)
        {
            logDebug("update failed: %s", e);
            return false;
        }
    }

	bool have(string filename) const nothrow
	{
        try
        {
			bool ret;

            auto talkURL = this.url ~ PathEntry("have")
                ~ PathEntry(filename);
            logInfo("have: %s", talkURL);
            requestHTTP(talkURL,
                (scope req) { },
                (scope res)
				{
					auto line = (cast(string)res.bodyReader.readLine(size_t.max, "\n")).chomp;

					if (line == "YES")
					{
						ret = true;
					}
					else
					{
						ret = false;
					}
				});

            return ret;
        }
        catch (Exception e)
        {
            logDebug("have failed: %s", e);
            return false;
        }
    }

    int opCmp(const ref Node that) const pure nothrow @safe @nogc
    {
        return tuple(this.host, this.port, this.path)
            .opCmp(tuple(that.host, that.port, that.path));
    }

	bool opEquals(const ref Node that) const pure nothrow @safe @nogc
	{
        return tuple(this.host, this.port, this.path)
            .opEquals(tuple(that.host, that.port, that.path));
	}

	size_t toHash() const nothrow @safe
	{
        return tuple(this.host, this.port, this.path).toHash();
	}
}

unittest
{
    const Node node = "example.com:8080/server.cgi";
    assert(node.host == "example.com");
    assert(node.port == 8080);
    assert(node.path == "/server.cgi");
    assert(node.toString == "example.com:8080/server.cgi");
    assert(node.url == URL("http://example.com:8080/server.cgi"));
    assert(node.url ~ PathEntry("ping") ==
        URL("http://example.com:8080/server.cgi/ping"));

    // may not compatible with shinGETsu protocol, but works with saku
    assert(node.url ~ PathEntry("join") ~ PathEntry("example.com:8001+server.cgi") ==
        URL("http://example.com:8080/server.cgi/join/example.com%3A8001%2Bserver.cgi"));
}

private Tuple!(RecordHead, "recordHead", string[], "tags") parseRecentRecord(string line) @safe
{
    static void enforceParse(bool cond, lazy string msg)
    {
        enforce(cond, "Failed to parse record: " ~ msg);
    }

    RecordHead head;

    enforceParse(!line.empty, "empty");
    auto fields = line.splitter("<>");

    // Feeding empty string to splitter gives empty range.
    assert(!fields.empty);
    head.stamp = fields.front.to!long;
    fields.popFront();

    enforceParse(!fields.empty, "missing id");
    head.id = fields.front;
    enforceParse(isValidRecordId(head.id), "invalid record id");
    fields.popFront();

    enforceParse(!fields.empty, "missing filename");
    head.filename = fields.front;
    // enforceParse(isValidThreadFileName(head.filename), "invalid file");
    fields.popFront();

    if (!fields.empty)
    {
        auto tagspart = fields.front;
        tagspart.skipOver("tags:");

        return typeof(return)(head, tagspart.split(' '));
    }
    else
    {
        return typeof(return)(head, null);
    }
}

unittest
{
    auto l = "1451837925<>ab46e150dec86f35df3a54920388e54d<>thread_E38090E3819FE381A0E381B2E3819FE3819" ~
        "9E38289E69BB8E3818DE8BEBCE38280E382B9E383ACE38091<>tags:雑談 Talk";
    auto res = parseRecentRecord(l);
    assert(res.tags == ["雑談", "Talk"]);

    res = parseRecentRecord("1452195302<>752fe5d88f64e40bf63259828bf5471b<>thread_E99BBBE6B3A2E382BDE383B3E382B0");
    assert(res.tags.empty);
}
