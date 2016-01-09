module novluno.service;

import novluno.cache;
import novluno.config;
import novluno.status;

import optional;

import vibe.core.core;
import vibe.core.log;

import std.algorithm;
import std.functional;
import std.random;
import std.range;
import core.time;

void runPingCron()
{
    static void ping()
    {
        logDebug("ping: timeout");

        try
        {
            foreach (node; g_status.linkedNodes.reader() ~ g_status.searchNodes.reader())
            {
                immutable p = node.ping();

                if (!p.success)
                {
                    g_status.linkedNodes.writer().removeKey(node);
                    g_status.searchNodes.writer().removeKey(node);
                }
            }

            g_status.linkedNodes.write((ref ln)
            {
                if (ln.length < config.numLinkedNodes)
                {
                    foreach (n; g_status.searchNodes.reader()[].randomCover
                            .filter!(n => n !in ln)
                            .take(config.numLinkedNodes - ln.length))
                    {
                        ln ~= n;
                    }
                }
            });

            g_status.searchNodes.write((ref sn)
            {
                if (sn.length > config.numSearchNodes)
                {
                    foreach (n; sn[].randomCover.take(sn.length - config.numLinkedNodes))
                    {
                        sn.removeKey(n);
                    }
                }
            });
        }
        catch (Exception e)
        {
            logDebug("ping: failed %s", e);
        }

        logDebug("ping: done");

        setTimer(config.pingInterval, toDelegate(&ping), false);
    }

    ping();
}

void runSyncCron()
{
    static void sync()
    {
        logDebug("sync: timeout");

        foreach (node; g_status.linkedNodes.reader())
        {
			logDebug("sync: %s", node.toString);

			try
			{
				import std.datetime : Clock;
				node.recent(
					Optional!long(Clock.currTime().toUnixTime() -
						config.syncRange.total!"seconds"),
					Optional!long())
					.filter!(r => isValidThreadFileName(r.filename))
					.each!((r)
						{
							g_cache.updateRecent(r);

							if (g_cache.hasFile(r.filename) && !g_cache.hasRecord(r))
							{
								g_status.get(r);
							}
						});
			}
			catch (Exception e)
			{
				logDebug("sync: failed %s", e);
			}
        }

        logDebug("sync: done");

        setTimer(config.syncInterval, toDelegate(&sync), false);
    }

    sync();
}
