module novluno.startup;

import novluno.cache;
import novluno.config;
import novluno.gateway;
import novluno.node;
import novluno.server;
import novluno.service;
import novluno.status;

import vibe.core.log;
import vibe.http.router;
import vibe.http.server;
import vibe.web.web;
import optional.util : optSwitch;

import std.algorithm;
import std.range;
import std.conv;
import std.exception;
import std.typecons;

shared static this()
{
    version (unittest)
    {
        logInfo("Skipping startup");

        // Just make sure the code below compiles
        if ((() => true)()) return;
    }

    auto router = new URLRouter;

    auto serverWebSettings = new WebInterfaceSettings;
    serverWebSettings.urlPrefix = config.serverPath;
    serverWebSettings.ignoreTrailingSlash = true;

    auto gatewayWebSettings = new WebInterfaceSettings;
    gatewayWebSettings.urlPrefix = config.gateway.path;
    gatewayWebSettings.ignoreTrailingSlash = true;

    router.registerWebInterface(new ServerWebInterface(),
        serverWebSettings)
        .registerWebInterface(new GatewayWebInterface(), gatewayWebSettings)
        .rebuild();

    auto settings = new HTTPServerSettings;
    with (settings)
    {
        accessLogToConsole = true;
        bindAddresses = ["0.0.0.0"]; // shinGETsu doesn't support IPv6
        port = config.port;
        useCompressionIfPossible = true;
        serverString = config.serverString;
        options = HTTPServerOption.defaults & ~HTTPServerOption.parseJsonBody
            | HTTPServerOption.distribute;
    }

    listenHTTP(settings, router);

    logInfo("Novluno is running. Open http://localhost:" ~ config.port.to!string
        ~ " in your browser.");

	//version(none)
	foreach (n; config.initialNodes.map!(n => Node(n)))
	{
		g_status.join(n);
	}


    import vibe.core.core : runTask, sleep;
    //version (none)
	runTask({
        import core.time : minutes;

        sleep(1.minutes);

        runPingCron();
        runSyncCron();
    });


    debug
    {
        listenTCP(cast(ushort)(config.port + 100), (conn)
        {
            import vibe.stream.operations;
            import std.string;
            import std.stdio;

            conn.write("Novluno debugging interface\n");

            while (conn.connected)
            {
                conn.write("> ");
                auto line = cast(string) conn.readLine(size_t.max, "\n");
                immutable sp = line.indexOf(' ');
                auto cmd = sp == -1 ? line : line[0 .. sp];
                if (sp == -1 || sp == line.length - 1)
                    line = null;
                else
                    line = line[sp + 1 .. $];

                try
                {
                    switch (cmd)
                    {
                    case "join":
                        g_status.join(Node(line));
                        break;

                    case "bye":
                        auto node = Node(line);
                        if (node.bye())
                            g_status.linkedNodes.writer().removeKey(node);
                        else
                            conn.write("failed\n");
                        break;

                    case "list":
                        conn.write("linked:\n");
                        conn.write(g_status.linkedNodes.reader()[]
                            .map!(n => n.toString()).join('\n') ~ "\n");
                        conn.write("\nsearch:\n");
                        conn.write(g_status.searchNodes.reader()[]
                            .map!(n => n.toString()).join('\n') ~ "\n");
                        break;

                    default:
                        conn.write("Unknown command: " ~ cmd ~ "\n");
                    }
                }
                catch (Exception e)
                {
                    conn.write(e.toString() ~ "\n");
                }
            }
        }

        );
    }
}
