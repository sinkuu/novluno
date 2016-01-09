module novluno.gateway;

import novluno.cache;
import novluno.config;
import novluno.status;

import optional.type;

import vibe.inet.path;
import vibe.core.log;
import vibe.http.server;
import vibe.web.web;

import std.array;
import std.conv;
import std.datetime;
import std.digest.digest;
import std.digest.md;
import std.typecons;

final class GatewayWebInterface
{
	static bool authRead(HTTPServerRequest req, HTTPServerResponse res)
	{
		return true;
	}

	static bool authWrite(HTTPServerRequest req, HTTPServerResponse res)
	{
		return true;
	}

	static bool authAdmin(HTTPServerRequest req, HTTPServerResponse res)
	{
		return true;
	}

	@before!authRead("auth")
	{
		void index(bool auth, HTTPServerResponse res)
		{
			auto heads = g_cache.getRecentByRange(
					Clock.currTime().toUnixTime - config.gateway.recentRange.total!"seconds",
					Clock.currTime().toUnixTime() + config.updateRangeFuture.total!"seconds");
			res.render!("top.dt", heads);
		}

		void getRecent(bool auth, HTTPServerResponse res)
		{
			auto heads = g_cache.getRecentByRange(0,
					Clock.currTime().toUnixTime() + config.updateRangeFuture.total!"seconds");
			res.render!("recent.dt", heads);
		}

		@path("/thread/:title")
		void getThread(bool auth, HTTPServerResponse res, string _title, long page = 0)
		{
			enforceHTTP(page >= 0, HTTPStatus.internalServerError);
			auto filename = "thread_" ~ encodeTitle(_title);

			Record[] records;
			bool exists;
			if (!g_cache.hasFile(filename))
			{
				auto curUnixTime = Clock.currTime().toUnixTime;

				foreach (n; g_status.linkedNodes.reader() ~ g_status.searchNodes.reader())
				{
					if (n.have(filename))
					{
						auto rs = n.get(filename, Optional!(long)(curUnixTime - config.getRange.total!"seconds"));
						if (!rs.empty)
						{
							g_cache.addRecords(rs.get);
							exists = true;
						}

						break;
					}
				}

			}
			else
			{
				exists = true;
			}

			if (exists)
			{
				records = g_cache.getNLatestRecords(filename, config.gateway.numRecordsPerPage,
					config.gateway.numRecordsPerPage * page);
			}


			res.render!("thread.dt", filename, records);
		}
	}

	@before!authWrite("auth")
	{
		@method(HTTPMethod.POST)
		@path("/thread/:title")
		void postThread(bool auth, HTTPServerRequest req, HTTPServerResponse res,
				string filename, string name, string mail, string _title)
		{
			enforceHTTP(isValidThreadFileName(filename), HTTPStatus.badRequest, "Invalid filename");

			import vibe.textfilter.html : htmlEscapeMin;

			auto body_ = htmlEscapeMin(req.form["body"]).replace("\r\n", "<br>").replace("\n", "<br>");
			name = htmlEscapeMin(name);
			mail = htmlEscapeMin(mail);

			Record record;

			auto str = appender!string();
			if (!name.empty)
			{
				str ~= "name:" ~ name ~ "<>";
				record.name = name;
			}

			if (!mail.empty)
			{
				str ~= "mail:" ~ mail ~ "<>";
				record.mail = mail;
			}

			str ~= "body:" ~ body_;
			record.body_ = body_;

			with (record)
			{
				head.filename = filename;
				head.stamp = Clock.currTime.toUnixTime;
				head.id = md5Of(str.data).toHexString!(LetterCase.lower);
			}

			g_cache.addRecord(record);
			g_cache.updateRecent(record.head);
			g_status.update(record.head);

			redirect((Path(config.gateway.path) ~ PathEntry("thread") ~ PathEntry(_title)).toString);
		}
	}

	@before!authAdmin("auth")
	{
		void getStatus(bool auth)
		{
		}
	}
}
