module novluno.cache;

import novluno.config;

import optional;
import vibe.core.log;
import vibe.core.core;
import vibe.core.stream;

import std.algorithm;
import std.array;
import std.ascii;
import std.conv;
import std.exception;
import std.functional;
import std.range;
import std.traits;
import std.typecons;

TaskLocal!Cache g_cache;

static this()
{
    g_cache = new SQLiteCache(config.cachePath);
}

/// Internal representation of a header for a Shingetsu record.
struct RecordHead
{
    long stamp;
    string id;
    string filename;
}

/// Internal representation of a Shingetsu record.
struct Record
{
    RecordHead head;

    Optional!string pubkey;
    Optional!string sign;
    Optional!string target;
    Optional!long remove_stamp;
    Optional!string remove_id;

    // thread
    Optional!string name;
    Optional!string mail;
    string body_;
    Optional!string suffix;
    Optional!(ubyte[]) attach;

    string recordStr;
}

private enum FieldType
{
    // Note: fields are generated in this order
    name,
    mail,
    body_,
    attach,
    suffix,
    pubkey,
    sign,
    target,
    remove_stamp,
    remove_id,
}


/// Convert the internal record representation to Shingetsu protocol format
void toShingetsuRecord(alias sink)(auto ref const Record record)
{
    toShingetsuHead!sink(record.head);

    sink("<>");

    bool putSep;

    foreach (f; EnumMembers!FieldType)
    {
        enum name = (s => s[$ - 1] == '_' ? s[0 .. $ - 1] : s)(f.to!string);
        auto field = __traits(getMember, record, text(f));
        static if (is(typeof(field) : Optional!T, T))
        {
            if (!field.empty)
            {
                if (putSep) sink("<>");
                sink(name ~ ":");

                static if (name == "attach")
                {
                    import std.base64;

                    string s = Base64.encode(field.get);
                    sink(s);
                }
                else
                {
                    static assert(!(isArray!T && !isSomeString!T));
                    sink(field.get.to!string);
                }
                putSep = true;
            }
        }
        else
        {
            //static if (is(typeof(field) : string))
            //    if (field is null) continue;

            if (putSep) sink("<>");
            sink(name ~ ":");
            sink(field.to!string);
            putSep = true;
        }
    }
}

/// ditto
string toShingetsuRecord()(auto ref const Record record) @safe pure nothrow
{
    auto app = appender!string();
    record.toShingetsuRecord!(s => app.put(s))();
    return app.data();
}

/// Convert the internal record head representation to Shingetsu protocol format.
string toShingetsuHead()(auto ref const RecordHead head) @safe pure nothrow
{
    return head.stamp.text ~ "<>" ~ head.id;
}

/// ditto
void toShingetsuHead(alias sink)(auto ref const RecordHead head)
{
    sink(head.stamp.text);
    sink("<>");
    sink(head.id);
}

@safe pure nothrow unittest
{
    Record record;
    with (record)
    {
        head.filename = "thread_AA";
        head.id = "b1946ac92492d2347c6235b4d2611184";
        head.stamp = 100;
        body_ = "hello";
    }

    assert(toShingetsuRecord(record) ==
            "100<>b1946ac92492d2347c6235b4d2611184<>body:hello");
}

/// Parses a record in Shingetsu protocol format.
Record parseShingetsuRecord(string recordStr, string filename) @safe pure
{
    assert(isValidThreadFileName(filename));
    assert(recordStr.all!(c => c != '\r' && c != '\n'));

    static void enforceParse(bool cond, lazy string msg)
    {
        enforce(cond, "Failed to parse record: " ~ msg);
    }

    Record rec;
    rec.head.filename = filename;
    rec.recordStr = recordStr;

    enforceParse(!recordStr.empty, "empty");
    auto fields = recordStr.splitter("<>");

    // Feeding empty string to splitter gives empty range.
    assert(!fields.empty);
    rec.head.stamp = fields.front.to!long;
    fields.popFront();

    enforceParse(!fields.empty, "missing id");
    rec.head.id = fields.front;
    enforceParse(isValidRecordId(rec.head.id), "invalid record id");
    fields.popFront();

    bool[FieldType] filled;

    foreach (f; fields)
    {
        immutable sp = f.findSplit(":");
        enforceParse(sp.length == 3, "broken field");

Lswitch:
        switch (sp[0])
        {
            foreach (ft; EnumMembers!FieldType)
            {
                enum name = (s => s[$ - 1] == '_' ? s[0 .. $ - 1] : s)(ft.to!string);

                case name:
                    enforceParse(!filled.get(ft, false), "duplicated field '" ~ sp[0] ~ "'");
                    filled[ft] = true;

                    static if (ft == FieldType.attach)
                    {
                        import std.base64;
                        rec.attach = Base64.decode(sp[2]);
                    }
                    else
                    {
                        alias FT = typeof(__traits(getMember, rec, text(ft)));
                        static if (is(FT : string))
                        {
                            __traits(getMember, rec, text(ft)) = sp[2];
                        }
                        else static if (is(FT : Optional!T, T))
                        {
                            __traits(getMember, rec, text(ft)) = sp[2].to!T;
                        }
                    }
                    break Lswitch;
            }

            default:
                enforceParse(false, "unknown field '" ~ sp[0] ~ "'");
                break;
        }
    }

    enforceParse(allOrNotAtAll(filled.get(FieldType.attach, false),
        filled.get(FieldType.suffix, false)),
        "no suffix for the attachment");

    enforceParse(allOrNotAtAll(filled.get(FieldType.pubkey, false),
        filled.get(FieldType.sign, false),
        filled.get(FieldType.target, false)),
        "signature fields are not complete");

    enforceParse(allOrNotAtAll(filled.get(FieldType.remove_stamp, false),
        filled.get(FieldType.remove_id, false)),
        "removal fields are not complete");

    return rec;
}

unittest
{
    Record record;
    with (record)
    {
        head.filename = "thread_AA";
        head.id = "b1946ac92492d2347c6235b4d2611184";
        head.stamp = 100;
        body_ = "hello http://";
        recordStr = head.stamp.text ~ "<>" ~ head.id ~ "<>body:" ~ body_;
    }

    assert(record.toShingetsuRecord.parseShingetsuRecord(record.head.filename) == record);

    assertThrown(parseShingetsuRecord("100<>5abc", "thread_AA"));
    assertNotThrown(parseShingetsuRecord("100<>" ~ record.head.id, "thread_AA"));
    assertThrown(parseShingetsuRecord("100<>" ~ record.head.id ~ "<>remove_stamp:200", "thread_AA"));
    assertNotThrown(parseShingetsuRecord("100<>" ~ record.head.id ~
        "<>remove_stamp:200<>remove_id:0e3ea802f68f93aefee15d998a4860c5", "thread_AA"));
}

interface Cache
{
    Cache clone();

    bool hasFile(string filename);
    bool hasRecord(RecordHead head);

    long getFileLength(string filename);
    Record getRecord(RecordHead head);
    Record[] getRecordsByRange(string filename, long beginTime, long endTime);
    Record[] getNLatestRecords(string filename, long num, long offset = 0);

    RecordHead[] getRecordHeadsByRange(string filename, long beginTime, long endTime);

    string getRecordString(RecordHead head);
    string[] getRecordStringsByRange(string filename, long beginTime, long endTime);

    void addRecord(Record record);
    void addRecords(Record[] records);

    ubyte[] getAttach(RecordHead head);

    RecordHead[] getRecentByRange(long beginTime, long endTime);
    void updateRecent(RecordHead head);
}

/+
final class SakuCache : Cache
{
    import std.file;
    import std.path;

    private immutable string _path;

    this(string path)
    {
        if (!exists(path)) mkdirRecurse(path);
        _path = path;
    }

    override bool hasFile(string filename)
    {
        assert(isValidThreadFileName(filename));

        immutable fpath = buildPath(_path, filename);
        return exists(fpath) && isDir(fpath);
    }

    override bool hasRecord(RecordHead head)
    {
        assert(isValidThreadFileName(head.filename));
        assert(isValidRecordId(head.filename));

        immutable rpath = buildPath(_path, head.filename, "record",
            head.stamp.to!string ~ "_" ~ head.id);

        return exists(rpath) && isFile(rpath);
    }

    Record getRecord(RecordHead head)
    {
        assert(isValidThreadFileName(head.filename));
        assert(isValidRecordId(head.filename));

        immutable rpath = buildPath(_path, head.filename, "record",
            head.stamp.text ~ "_" ~ head.id);

        // parse
    }

    Record[] getRecordsByRange(string filename, long beginTime, long endTime);

    string getRecordString(RecordHead head);

    string[] getRecordStringsByRange(string filename, long beginTime, long endTime);

    void addRecord(Record record, ubyte[] attach = null);

    void addRecord(RecordHead head, string recordStr);

    ubyte[] getAttach(RecordHead head);

    RecordHead[] getRecentByRange(long beginTime, long endTime);

    void updateRecent(RecordHead head);
}
+/

final class SQLiteCache : Cache
{
    import d2sqlite3;

    private
    {
        immutable string _path;
        Database _db;
        Statement stFileKeyByName;
        Statement stFileExistenceByName;
        Statement stRecord;
        Statement stRecordStr;
        Statement stRecordExistence;
        Statement stRecordsByRange;
        Statement stNLatestRecords;
        Statement stRecordHeadsByRange;
        Statement stRecordStrByRange;
        Statement stAttach;
        Statement stRecentByRange;
        Statement stUpdateRecent;
        Statement stRecentByFile;
        Statement stRecordInsert;
        Statement stFileInsert;
        Statement stFileLength;
    }


    this(string path)
    {
        _path = path;

        import std.file : exists;

        immutable needsInit = _path == ":memory:" || !exists(_path);

        _db = Database(_path);

        if (needsInit)
        {
            _db.execute("BEGIN");

            _db.execute("CREATE TABLE IF NOT EXISTS file (
                key INTEGER PRIMARY KEY,
                filename TEXT NOT NULL UNIQUE)");

            _db.execute("CREATE INDEX fileIndex ON file(filename)");

            _db.execute("CREATE TABLE IF NOT EXISTS record (
                stamp INTEGER NOT NULL,
                id TEXT NOT NULL,
                fileKey INTEGER NOT NULL,
                record_str TEXT NOT NULL,

                pubkey TEXT,
                sign TEXT,
                target TEXT,
                remove_stamp INTEGER,
                remove_id TEXT,

                name TEXT,
                mail TEXT,
                body TEXT,
                attach BLOB,
                suffix TEXT,

                FOREIGN KEY(fileKey) REFERENCES file(key),
                PRIMARY KEY(stamp, id, fileKey)
                )");

            _db.execute("CREATE INDEX recordIndex ON record(fileKey, stamp ASC)");

            _db.execute("CREATE TABLE IF NOT EXISTS recent (
                stamp INTEGER NOT NULL,
                id TEXT NOT NULL,
                filename TEXT NOT NULL PRIMARY KEY
                )");

            _db.execute("CREATE INDEX recentIndex ON recent(stamp ASC)");

            _db.commit();
        }

        initializePreparedStatements();
    }

    this(Database db, string path)
    {
        _db = db;
        _path = path;
        initializePreparedStatements();
    }

    private void initializePreparedStatements()
    {
        stFileKeyByName = _db.prepare(
            "SELECT key FROM file WHERE filename = :filename LIMIT 1");
        stFileExistenceByName = _db.prepare("SELECT EXISTS
            (SELECT 1 FROM file WHERE filename = :filename LIMIT 1)");
        stRecord = _db.prepare("SELECT * FROM record
            INNER JOIN file ON file.key = record.fileKey
            WHERE file.filename = :filename AND record.id = :record_id
            LIMIT 1");
    stRecordStr = _db.prepare("SELECT record_str FROM record
            INNER JOIN file ON file.key = record.fileKey
            WHERE file.filename = :filename AND record.id = :record_id
            LIMIT 1");
        stRecordExistence = _db.prepare("SELECT EXISTS
            (SELECT 1
                FROM record
                INNER JOIN file ON file.key = record.fileKey
                WHERE file.filename = :filename AND record.id = :recordid
                LIMIT 1)");
        stRecordsByRange = _db.prepare("SELECT *
            FROM record
            INNER JOIN file ON file.key = record.fileKey
            WHERE filename = :filename AND :beginTime <= record.stamp AND record.stamp <= :endTime
            ORDER BY record.stamp DESC");
        stNLatestRecords = _db.prepare("SELECT * FROM record
            INNER JOIN file ON file.key = record.fileKey
            WHERE file.filename = :filename
            ORDER BY stamp DESC
            LIMIT :num OFFSET :offset");
            // CASE WHEN attach IS NULL THEN 0 ELSE 1 END hasAttach
        stRecordHeadsByRange = _db.prepare("SELECT
            id, stamp
            FROM record
            INNER JOIN file ON file.key = record.fileKey
            WHERE file.filename = :filename AND :beginTime <= record.stamp AND record.stamp <= :endTime
            ORDER BY stamp DESC");
        stRecordStrByRange = _db.prepare("SELECT record_str
            FROM record
            INNER JOIN file ON file.key = record.fileKey
            WHERE file.filename = :filename AND :beginTime <= record.stamp AND record.stamp <= :endTime
            ORDER BY stamp DESC");
        stAttach = _db.prepare("SELECT attach
            FROM record
            INNER JOIN file ON file.key = record.fileKey
            WHERE file.filename = :filename AND id = :recordid LIMIT 1");
        stRecentByRange = _db.prepare("SELECT stamp, id, filename
            FROM recent
            WHERE :beginTime <= stamp AND stamp <= :endTime
            ORDER BY stamp ASC");
        stUpdateRecent = _db.prepare("
            REPLACE
                INTO recent(filename, stamp, id)
                VALUES (:filename, :stamp, :id)");
        stRecentByFile = _db.prepare("SELECT stamp FROM recent
            WHERE filename = :filename");
        stRecordInsert = _db.prepare("INSERT
            INTO record(fileKey, stamp, id, record_str, pubkey, sign, target, remove_stamp,
            remove_id, name, mail, body, attach, suffix)
            VALUES (:fileKey, :stamp, :record_id, :record_str, :pubkey, :sign, :target, :remove_stamp,
            :remove_id, :name, :mail, :body, :attach, :suffix)");
        stFileInsert = _db.prepare("INSERT INTO file
            (filename) VALUES (:filename)");
        stFileLength = _db.prepare("SELECT count() FROM record
            INNER JOIN file ON file.key = record.fileKey
            WHERE file.filename = :filename");
    }

    private static Record rowToRecord(Row row, string filename)
    {
        Record r;

        with (r)
        {
            head = rowToRecordHead(row, filename);

            pubkey = row.peek!(Nullable!string)("pubkey").fromNullable();
            sign = row.peek!(Nullable!string)("sign").fromNullable();
            target = row.peek!(Nullable!string)("target").fromNullable();
            remove_stamp = row.peek!(Nullable!long)("remove_stamp").fromNullable();
            remove_id = row.peek!(Nullable!string)("remove_id").fromNullable();

            name = row.peek!(Nullable!string)("name").fromNullable();
            mail = row.peek!(Nullable!string)("mail").fromNullable();
            body_ = row.peek!string("body");

            attach = row.peek!(Nullable!(ubyte[]))("attach").fromNullable();
            suffix = row.peek!(Nullable!string)("suffix").fromNullable();

            recordStr = row.peek!string("record_str");
        }

        return r;
    }

    private static RecordHead rowToRecordHead(Row row, string filename)
    {
        RecordHead r;

        r.filename = filename;
        r.stamp = row.peek!long("stamp");
        r.id = row.peek!string("id");

        return r;
    }

    Cache clone()
    {
        return new SQLiteCache(_db, _path);
    }

    override bool hasFile(string filename)
    {
        assert(isValidThreadFileName(filename));

        stFileExistenceByName.bindAll(filename);
        scope (exit) stFileExistenceByName.reset();
        immutable ret = stFileExistenceByName.execute().oneValue!bool;
        return ret;
    }

    override bool hasRecord(RecordHead head)
    {
        assert(isValidThreadFileName(head.filename));
        assert(isValidRecordId(head.id));

        stRecordExistence.bindAll(head.filename, head.id);
        scope (exit) stRecordExistence.reset();
        immutable ret = stRecordExistence.execute().oneValue!bool;
        return ret;
    }

    override Record getRecord(RecordHead head)
    {
        assert(isValidThreadFileName(head.filename));
        assert(isValidRecordId(head.id));

        stRecord.bindAll(head.filename, head.id);
        scope (exit) stRecord.reset();
        auto r = rowToRecord(stRecord.execute().front, head.filename);
        return r;
    }

    override Record[] getRecordsByRange(string filename, long beginTime, long endTime)
    {
        assert(isValidThreadFileName(filename));
        assert(beginTime <= endTime);

        stRecordsByRange.bindAll(filename, beginTime, endTime);
        scope (exit) stRecordsByRange.reset();

        auto records = stRecordsByRange.execute()
            .map!(r => rowToRecord(r, filename)).array;

        return records;
    }

    override long getFileLength(string filename)
    {
        assert(isValidThreadFileName(filename));

        stFileLength.bindAll(filename);
        scope (exit) stFileLength.reset();
        auto len = stFileLength.execute().oneValue!long;

        return len;
    }

    override Record[] getNLatestRecords(string filename, long num, long offset = 0)
    {
        assert(num >= 0 && offset >= 0);
        assert(isValidThreadFileName(filename));

        stNLatestRecords.bindAll(filename, num, offset);
        scope (exit) stNLatestRecords.reset();

        auto records = stNLatestRecords.execute()
            .map!(r => rowToRecord(r, filename)).array;
        return records;
    }

    override RecordHead[] getRecordHeadsByRange(string filename, long beginTime, long endTime)
    {
        assert(isValidThreadFileName(filename));
        assert(beginTime <= endTime);

        stRecordHeadsByRange.bindAll(filename, beginTime, endTime);
        scope (exit) stRecordHeadsByRange.reset();
        auto records = stRecordHeadsByRange.execute()
            .map!(r => rowToRecordHead(r, filename)).array;
        return records;
    }

    string getRecordString(RecordHead head)
    {
        assert(isValidThreadFileName(head.filename));
        assert(isValidRecordId(head.id));

        stRecordStr.bindAll(head.filename, head.id);
        scope (exit) stRecordStr.reset();
        immutable ret = stRecordStr.execute().oneValue!string;

        return ret;
    }

    string[] getRecordStringsByRange(string filename, long starttime, long endtime)
    {
        assert(isValidThreadFileName(filename));
        assert(starttime <= endtime);

        stRecordStrByRange.bindAll(filename, starttime, endtime);
        scope (exit) stRecordStrByRange.reset();
        auto records = stRecordStrByRange.execute().map!(r => r.peek!string(0)).array;

        return records;
    }

    override void addRecord(Record record)
    {
        assert(isValidThreadFileName(record.head.filename));
        assert(isValidRecordId(record.head.id));

        _db.execute("BEGIN");
        long fileKey;
        stFileKeyByName.bindAll(record.head.filename);
        scope (exit) stFileKeyByName.reset();
        auto res = stFileKeyByName.execute();
        if (res.empty)
        {
            stFileInsert.inject(record.head.filename);

            stFileKeyByName.reset();
            stFileKeyByName.bindAll(record.head.filename);
            fileKey = stFileKeyByName.execute().oneValue!long;
        }
        else
        {
            fileKey = res.oneValue!long;
        }

        auto recordStr = record.toShingetsuRecord();

        //(:fileKey, :stamp, :record_id, :record_str, :pubkey, :sign, :target,
        // :remove_stamp, :remove_id, :name, :mail, :body, :attach)");
        stRecordInsert.inject(fileKey, record.head.stamp, record.head.id, recordStr, cast(
            Nullable!string) record.pubkey, cast(Nullable!string) record.sign,
            cast(Nullable!string) record.target,
            cast(Nullable!long) record.remove_stamp, cast(Nullable!string) record.remove_id,
            cast(Nullable!string) record.name,
            cast(Nullable!string) record.mail,
            record.body_,
            cast(Nullable!(ubyte[])) record.attach,
            cast(Nullable!string) record.suffix);
        _db.commit();
    }

    override void addRecords(Record[] records)
    {
        if (records.empty) return;

        _db.execute("BEGIN");
        long fileKey;
        stFileKeyByName.bindAll(records[0].head.filename);
        scope (exit) stFileKeyByName.reset();
        auto res = stFileKeyByName.execute();
        if (res.empty)
        {
            stFileInsert.inject(records[0].head.filename);

            stFileKeyByName.reset();
            stFileKeyByName.bindAll(records[0].head.filename);
            fileKey = stFileKeyByName.execute().oneValue!long;
        }
        else
        {
            fileKey = res.oneValue!long;
        }

        foreach (record; records)
        {
            assert(isValidThreadFileName(record.head.filename));
            assert(isValidRecordId(record.head.id));

            auto recordStr = record.toShingetsuRecord();

            //(:fileKey, :stamp, :record_id, :record_str, :pubkey, :sign, :target,
            // :remove_stamp, :remove_id, :name, :mail, :body, :attach)");
            stRecordInsert.inject(fileKey, record.head.stamp, record.head.id, recordStr, cast(
                    Nullable!string) record.pubkey, cast(Nullable!string) record.sign,
                    cast(Nullable!string) record.target,
                    cast(Nullable!long) record.remove_stamp, cast(Nullable!string) record.remove_id,
                    cast(Nullable!string) record.name,
                    cast(Nullable!string) record.mail,
                    record.body_,
                    cast(Nullable!(ubyte[])) record.attach,
                    cast(Nullable!string) record.suffix);
        }

        _db.commit();
    }

    override ubyte[] getAttach(RecordHead head)
    {
        assert(isValidThreadFileName(head.filename));
        assert(isValidRecordId(head.id));

        stAttach.bindAll(head.filename, head.id);
        scope (exit) stAttach.reset();
        auto a = stAttach.execute().oneValue!(ubyte[]);
        return a;
    }

    override RecordHead[] getRecentByRange(long beginTime, long endTime)
    {
        assert(beginTime <= endTime);

        stRecentByRange.bindAll(beginTime, endTime);
        scope (exit) stRecentByRange.reset();
        auto records = stRecentByRange.execute()
            .map!(r => rowToRecordHead(r, r.peek!string("filename"))).array;
        return records;
    }

    override void updateRecent(RecordHead record)
    {
        _db.execute("BEGIN");
        stRecentByFile.bindAll(record.filename);
        scope (exit) stRecentByFile.reset();
        auto rf = stRecentByFile.execute();
        if (rf.empty || rf.front.peek!long("stamp") < record.stamp)
            stUpdateRecent.inject(record.filename, record.stamp, record.id);
        _db.commit();
    }

    // TODO:
    version(none)
    static void migrateFromSakuCache(string sakuPath, string sqlitePath)
    {
        auto saku = new SakuCache(sakuPath);
        auto sqlite = new SQLiteCache(sqlitePath);
    }
}

unittest
{
    import d2sqlite3;

    try
    {
        enum unittestDB = ":memory:";

        {
            import std.file;
            if (unittestDB != ":memory:" && exists(unittestDB)) remove(unittestDB);
        }

        auto cache = new SQLiteCache(unittestDB);

        Record r1, r2, r3, r4;
        with (r1)
        {
            head.filename = "thread_AA";
            head.id = "b1946ac92492d2347c6235b4d2611184";
            head.stamp = 100;
            body_ = "";
        }
        with (r2)
        {
            head.filename = "thread_AA";
            head.id = "12fc204edeae5b57713c5ad7dcb97d39";
            head.stamp = 200;
            mail = "sage";
            body_ = "hai";
        }
        with (r3)
        {
            head.filename = "thread_AA";
            head.id = "c34b28b9769ac33d25a31677f485a42a";
            head.stamp = 500;
            body_ = "添付";
            attach = cast(ubyte[]) "Hello";
            suffix = "txt";
        }
        with (r4)
        {
            head.filename = "thread_BB";
            head.id = "1ed1fc6080dd35a0a451dfd7cbba4e07";
            head.stamp = 800;
            body_ = "hello<br>world";
        }

        assert(!cache.hasFile("thread_AA"));
        cache.addRecord(r1);
        assert(cache.hasFile("thread_AA"));
        assert(cache.getFileLength("thread_AA") == 1);
        assert(cache.getRecord(r1.head).toShingetsuRecord
            == r1.toShingetsuRecord);

        assert(!cache.hasRecord(r2.head));
        cache.addRecord(r2);
        assert(cache.hasRecord(r2.head));
        assert(cache.getRecordString(r2.head) ==
                "200<>" ~ r2.head.id ~ "<>mail:sage<>body:hai");

        cache.addRecord(r3);
        immutable expected = "500<>" ~ r3.head.id ~ "<>body:添付<>attach:SGVsbG8=<>suffix:txt";
        assert(cache.getRecordString(r3.head) == expected);
        assert(cache.getRecord(r3.head).toShingetsuRecord == expected);
        assert(cache.getAttach(r3.head) == "Hello");
        assert(cache.getRecordStringsByRange("thread_AA", 500, 500)[0] == expected);

        cache.addRecord(r4);
        assert(cache.getRecord(r4.head).body_ == r4.body_);
        assert(cache.getFileLength("thread_AA") == 3);
        assert(cache.getRecordsByRange("thread_AA", 0, long.max).length == 3);

        assert(cache.getNLatestRecords("thread_AA", 2, 1)
            .map!(r => r.head.stamp).equal([200, 100]));

        assert(cache.getRecentByRange(0, long.max) == []);

        cache.updateRecent(r1.head);
        assert(cache.getRecentByRange(0, 500) == [RecordHead(100, r1.head.id, "thread_AA")]);

        cache.updateRecent(r2.head);
        assert(cache.getRecentByRange(0, 500) == [RecordHead(200, r2.head.id, "thread_AA")]);

        cache.updateRecent(r3.head);
        assert(cache.getRecentByRange(0, 500) == [RecordHead(500, r3.head.id, "thread_AA")]);

        cache.updateRecent(r4.head);
        assert(cache.getRecentByRange(0, 500) == [RecordHead(500, r3.head.id, "thread_AA")]);
        assert(cache.getRecentByRange(0, long.max) ==
            [RecordHead(500, r3.head.id, "thread_AA"), RecordHead(800, r4.head.id, "thread_BB")]);
    }
    catch (SqliteException e)
    {
        import std.stdio;

        stderr.writeln(e.sql);
        throw e;
    }
}

bool isValidThreadFileName(string filename) pure nothrow @safe @nogc
{
    import std.utf : byChar;
    import std.ascii : isHexDigit;

    if (!filename.startsWith("thread_"))
        return false;

    filename = filename["thread_".length .. $];

    return filename.length > 0 &&
        filename.length % 2 == 0 &&
        filename.byChar.all!(c => c.isHexDigit);
}

unittest
{
    assert( isValidThreadFileName("thread_E99B91E8AB87"));
    assert(!isValidThreadFileName("thread_E99B91E8AB8"));
    assert(!isValidThreadFileName("thread_"));
    assert(!isValidThreadFileName("thread"));
    assert(!isValidThreadFileName("thread_*"));
}

bool isValidRecordId(string id) pure nothrow @safe @nogc
{
    import std.utf : byChar;

    return id.length == 32 && id.byChar.all!(c => c.isDigit || ('a' <= c
        && c <= 'f'));
}

unittest
{
    assert(isValidRecordId("aecdffafc11b0a66621e1b373abaf693"));
    assert(!isValidRecordId("+ecdffafc11b0a66621e1b373abaf693"));
    assert(!isValidRecordId("1234ffff"));
}

S encodeTitle(S)(S title) if (isSomeString!S)
{
    import std.utf : validate;

    validate(title);

    import std.format : formattedWrite;

    auto app = appender!string;
    foreach (char c; title)
    {
        app.formattedWrite("%0X", c);
    }
    return app.data();
}

pure @safe unittest
{
    assert(encodeTitle("雑談") == "E99B91E8AB87");
}

S decodeTitle(S)(S title) @trusted if (isSomeString!S)
{
    static fromHexDigit(dchar c) pure
    {
        return isDigit(c) ? c - '0' : c - 'A' + 10;
    }

    import std.utf : byChar;

    auto str = title.byChar;

    auto app = appender!(ubyte[]);
    foreach (dchar a, dchar b; zip(StoppingPolicy.requireSameLength,
        str.stride(2), str.drop(1).stride(2)))
    {
        enforce(isHexDigit(a) && isHexDigit(b));
        app ~= cast(ubyte)(fromHexDigit(a) * 16 + fromHexDigit(b));
    }

    import std.utf : validate;

    validate(cast(string) app.data);

    return (cast(string) app.data).to!S;
}

pure @safe unittest
{
    assert(decodeTitle("E99B91E8AB87") == "雑談");
    assertThrown(decodeTitle("C0AF"));
}

private:

bool allOrNotAtAll(in bool[] conds...) pure nothrow @safe @nogc
{
    if (conds.length == 0) return true;
    if (conds[0])
        return conds[1 .. $].all;
    else
        return conds[1 .. $].all!(b => !b);
}

/+
pragma(inline)
bool implies(bool a, bool b) pure nothrow @safe @nogc
{

    return !a || b;
}
+/
