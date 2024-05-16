using System;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using FluentAssertions;
using Xunit;

namespace LiteDB.Tests.Database
{
    public class ConnectionString_Tests
    {
        [Fact]
        public void ConnectionString_Parser()
        {
            // only filename
            var onlyfile = new ConnectionString(@"demo.db");

            onlyfile.Filename.Should().Be(@"demo.db");

            // file with spaces without "
            var normal = new ConnectionString(@"filename=c:\only file\demo.db");

            normal.Filename.Should().Be(@"c:\only file\demo.db");

            // filename with timeout

            // file with spaces with " and ;
            var full = new ConnectionString(
                @"filename=""c:\only;file\""d\""emo.db""; 
                  readONLY =  TRUE;");

            full.Filename.Should().Be(@"c:\only;file""d""emo.db");
            full.ReadOnly.Should().BeTrue();
        }

        [Fact]
        public void ConnectionString_Very_Long()
        {
            var cn = new ConnectionString(@"Filename=C:\Users\yup\AppData\Roaming\corex\storecore.file;'");

            cn.Filename.Length.Should().Be(49);

        }
    }
}