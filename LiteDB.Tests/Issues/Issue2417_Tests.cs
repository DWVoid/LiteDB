using FluentAssertions;
using LiteDB.Engine;
using System;
using System.IO;
using System.Linq;

using Xunit;

namespace LiteDB.Tests.Issues
{
    public class Issue2417_Tests
    {
        [Fact]
        public void Rebuild_Detected_Infinite_Loop()
        {
            var original = "../../../Resources/Issue2417_MyData.db";

            using (var filename = new TempFile(original))
            {
                var settings = new EngineSettings
                {
                    Filename = filename,
                    AutoRebuild = true,
                };

                try
                {
                    using (var db = new LiteEngine(settings))
                    {
                        // infinite loop here
                        var col = db.Query("customers", Query.All()).ToList();

                        // never run here
                        Assert.Fail("not expected");
                    }
                }
                catch (Exception ex)
                {
                    Assert.True(ex is LiteException lex && lex.ErrorCode == 999);
                }

                using (var db = new LiteEngine(settings))
                {
                    var col = db.Query("customers", Query.All()).ToList().Count;
                    var errors = db.Query("_rebuild_errors", Query.All()).ToList().Count;

                    col.Should().Be(4);
                    errors.Should().Be(0);
                }
            }
        }
    }
}

