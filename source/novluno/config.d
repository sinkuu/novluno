module novluno.config;

import vibe.core.log;
import vibe.data.json;

import core.time;
import std.array;
import std.file;
import std.path;
import std.process;
import std.stdio;

// TODO: use SDL or something. JSON is not human-friendly
// FIXME: rewrite this source!! Follow normal UNIX application's behavior.

immutable Config config;

shared static this()
{
    debug
    {
        logInfo("Using default configuration");
    }
    else
    {
        config = loadConfig();
    }

    // override
    import vibe.core.args : readOption;
    readOption("port", cast(ushort*)&config.port, "port");
    readOption("cache", cast(string*)&config.cachePath, "cache");
}

struct Config
{
    string serverPath = "/server.cgi";
    string host = "";
    ushort port = 8002;

    //debug
	//	string[] initialNodes = ["localhost:8000/server.cgi"];
    //else
	string[] initialNodes = ["node.shingetsu.info:8000/server.cgi"];

    auto pingInterval = SerializableDuration(10.minutes);
    auto syncInterval = SerializableDuration(5.hours);

    auto updateRangePast = SerializableDuration(1.days);
    auto updateRangeFuture = SerializableDuration(30.minutes);

	auto getRange = SerializableDuration(30.days);
	auto syncRange = SerializableDuration(30.days);

    string cachePath = "cache.sqlite3";

    string serverString = "shinGETsu/0.7 (Saku/4.7.1)";

    uint numLinkedNodes = 5;
    uint numSearchNodes = 30;

    uint joinRetry = 2;
    uint pingRetry = 1;

	static struct Gateway
	{
		string path = "/";

		uint numRecordsPerPage = 30;

		auto recentRange = SerializableDuration(3.days);
	}

	Gateway gateway;
}

private struct SerializableDuration
{
    Duration _dur;
    alias _dur this;

    ulong toRepresentation() const pure nothrow @safe @nogc
    {
        return _dur.total!"seconds";
    }

    static auto fromRepresentation(ulong i) pure nothrow @safe @nogc
    {
        return SerializableDuration(i.seconds);
    }
}

static assert(isCustomSerializable!SerializableDuration);

/// Load configuration from configPath or default path
immutable(Config) loadConfig(string configPath = null)
{
    if (configPath is null)
        configPath = getConfigPath();

    if (!exists(configPath))
    {
        logInfo("configuration file existed. Creating one with default settings.");
        saveConfig(Config());
        return Config();
    }
    else
    {
        import std.exception : assumeUnique;

        auto configFile = File(configPath, "r");

        auto buf = uninitializedArray!(char[])(configFile.size);
        configFile.rawRead(buf);
        return deserializeJson!(immutable(Config))(assumeUnique(buf));
    }
}

void saveConfig(Config cfg, string configPath = null)
{
    if (configPath is null)
        configPath = getConfigPath();
    mkdirRecurse(configPath.dirName);

    auto configFile = File(configPath, "w");
    configFile.rawWrite(serializeToJson(cfg).toString());
}

private string getConfigPath() @safe
{
    // TODO: support other platforms

    if (auto confdir = environment.get("XDG_CONFIG_HOME", null))
    {
        return buildPath(confdir, "novluno", "novluno.json");
    }
    else if (auto home = environment.get("HOME", null))
    {
        return buildPath(home, ".config", "novluno", "novluno.json");
    }
    else
    {
        throw new Exception("Couldn't determine config directory");
    }
}
