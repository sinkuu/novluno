module novluno.status;

import novluno.cache;
import novluno.config;
import novluno.node;
import novluno.util.unorderedset;
import optional.type;

import vibe.core.log;
import vibe.core.core;
import vibe.core.sync : TaskReadWriteMutex;

import std.algorithm;

private __gshared Status _g_status;

pragma(inline)
Status g_status() @trusted @nogc nothrow
{
    return _g_status;
}

shared static this()
{
    _g_status = new Status;
}

final class Status
{
    ScopedReaderWriterLock!(UnorderedSet!Node) linkedNodes;
    ScopedReaderWriterLock!(UnorderedSet!Node) searchNodes;

    this()
    {
        linkedNodes = new typeof(linkedNodes)(UnorderedSet!Node());
        searchNodes = new typeof(searchNodes)(UnorderedSet!Node());
    }

    void join(Node n)
    {
        auto ret = n.join();
        if (ret.success)
        {
            linkedNodes.write((ref UnorderedSet!Node ln) {
                if (ln.length < config.numLinkedNodes)
                {
                    ln ~= n;
                }
            });
            searchNodes.writer() ~= n;

            if (!ret.node.empty)
            {
                logDebug("join: got another node");
                auto node = ret.node.get;

                if (node !in this.linkedNodes.reader())
                {
                    runTask({
                        if (linkedNodes.reader().length < config.numLinkedNodes)
                        {
                            this.join(ret.node.get);
                        }
                        else
                        {
                            if (ret.node.get.ping().success)
                            {
                                searchNodes.writer() ~= ret.node.get;
                            }
                        }
                    });
                }
            }
        }
    }

	void update(RecordHead h)
	{
		foreach (n; linkedNodes.reader())
		{
			runTask(
				{
					n.update(h, selfNode);
				});
		}
	}

	Optional!Record get(RecordHead head)
	{
		foreach (n; g_status.linkedNodes.reader() ~ g_status.searchNodes.reader())
		{
			if (!n.have(head.filename)) continue;

			auto r = n.get(head);
			if (!r.empty)
			{
				g_cache.addRecord(r.get);
				return Optional!Record(r.get);
			}
		}

		return Optional!Record();
	}
}

class ScopedReaderWriterLock(T) {
   private {
       TaskReadWriteMutex _mutex;
       T _payload;
   }
   
   this(T payload, TaskReadWriteMutex.Policy policy = TaskReadWriteMutex.Policy.PREFER_WRITERS)
   {
       import std.algorithm.mutation : move;

       _mutex = new TaskReadWriteMutex(policy);
       payload.move(_payload);
   }

   static struct ScopedReader
   {
       private TaskReadWriteMutex.Reader _reader;
       private const T* _payload;

       auto ref payload() return
       {
           return *_payload;
       }

       alias payload this;

       this(TaskReadWriteMutex.Reader reader, ref const T payload)
       {
           _reader = reader;
           _payload = &payload;
           _reader.lock();
       }

       ~this() {
           _reader.unlock();
       }
   }

   ScopedReader reader()
   {
       return ScopedReader(_mutex.reader, _payload);
   }

   void read(void delegate(ref const T) dg)
   {
        auto r = reader();
        dg(r);
   }

   static struct ScopedWriter
   {
       private TaskReadWriteMutex.Writer _writer;
       private T* _payload;

       auto ref payload() return
       {
           return *_payload;
       }

       alias payload this;

       this(TaskReadWriteMutex.Writer writer, ref T payload)
       {
           _writer = writer;
           _payload = &payload;
           _writer.lock();
       }

       ~this()
       {
           _writer.unlock();
       }
   }

   ScopedWriter writer()
   {
       return ScopedWriter(_mutex.writer, _payload);
   }

   void write(void delegate(ref T) dg)
   {
        auto w = writer();
        dg(w);
   }
}
