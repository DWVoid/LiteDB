﻿using LiteDB;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace LiteDB.Demo
{
    class Program
    {
        static void Main(string[] args)
        {
            var sw = new Stopwatch();

            var datafile = @"c:\temp\app.db";
            var walfile = @"c:\temp\app-wal.db";

            File.Delete(datafile);
            File.Delete(walfile);

            var log = new Logger(Logger.FULL, (s) => Console.WriteLine("> " + s));

            using (var db = new LiteEngine(new ConnectionString { Filename = datafile, Log = log }))
            {
                var ts = new List<Task>();
                sw.Start();

                db.EnsureIndex("col1", "name", new BsonExpression("$.name"), false);
                db.EnsureIndex("col2", "name", new BsonExpression("$.name"), false);

                //ts.Add(Task.Run(() =>
                //{
                    db.Insert("col1", ReadDocuments(), BsonAutoId.ObjectId);
                //}));
                //ts.Add(Task.Run(() =>
                //{
                    db.Insert("col2", ReadDocuments(), BsonAutoId.ObjectId);
                //}));

                Task.WaitAll(ts.ToArray());

                Console.WriteLine("Total (b/WAL): " + sw.ElapsedMilliseconds);

            }

            sw.Stop();
            Console.WriteLine("Total (a/WAL): " + sw.ElapsedMilliseconds);


            using (var db = new LiteEngine(datafile))
            {
                var s = db.Info();
                //Console.WriteLine(JsonSerializer.Serialize(db.Info(), true));

                var d = db.Find("col1", Query.EQ("_id", 3)).FirstOrDefault();

                Console.WriteLine(d?.ToString());
            }


            Console.WriteLine("End");
            Console.ReadKey();
        }

        static IEnumerable<BsonDocument> ReadDocuments()
        {
            var counter = 100;

            using (var s = File.OpenRead(@"datagen.txt"))
            {
                var r = new StreamReader(s);

                while(!r.EndOfStream && --counter > 0)
                {
                    var line = r.ReadLine();

                    if (!string.IsNullOrEmpty(line))
                    {
                        var row = line.Split(',');

                        yield return new BsonDocument
                        {
                            ["_id"] = Convert.ToInt32(row[0]),
                            ["name"] = row[1],
                            ["age"] = Convert.ToInt32(row[2])
                        };
                    }
                }
            }
        }
    }
}