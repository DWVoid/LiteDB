using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using LiteDB.Storage;
using static LiteDB.Constants;

namespace LiteDB.Engine
{
    /// <summary>
    /// All engine settings used to starts new engine
    /// </summary>
    public class EngineSettings
    {
        /// <summary>
        /// Get/Set custom stream to be used as temp file. If is null, will create new FileStreamFactory with "-tmp" on name
        /// </summary>
        public Stream TempStream { get; set; } = null;

        /// <summary>
        /// Full path or relative path from DLL directory. Can use ':temp:' for temp database or ':memory:' for in-memory database. (default: null)
        /// </summary>
        public string Filename { get; set; }

        /// <summary>
        /// Create database with custom string collection (used only to create database) (default: Collation.Default)
        /// </summary>
        public Collation Collation { get; set; }

        /// <summary>
        /// Indicate that engine will open files in readonly mode (and will not support any database change)
        /// </summary>
        public bool ReadOnly { get; set; } = false;

        /// <summary>
        /// After a Close with exception do a database rebuild on next open
        /// </summary>
        public bool AutoRebuild { get; set; } = false;

        /// <summary>
        /// Create new IRandomAccessFactory for datafile
        /// </summary>
        internal IRandomAccessFactory CreateDataFileFactory()
        {
            if (this.Filename == ":memory:")
            {
                throw new NotImplementedException();
            }

            if (this.Filename == ":temp:")
            {
                throw new NotImplementedException();
            }

            if (!string.IsNullOrEmpty(this.Filename))
            {
                return new RandomAccessFileFactory(this.Filename, this.ReadOnly);
            }

            throw new ArgumentException("EngineSettings must have Filename or DataStream as data source");
        }

        /// <summary>
        /// Create new IRandomAccessFactory for logfile
        /// </summary>
        internal IRandomAccessFactory CreateLogFileFactory()
        {
            if (this.Filename == ":memory:")
            {
                throw new NotImplementedException();
            }

            if (this.Filename == ":temp:")
            {
                throw new NotImplementedException();
            }

            if (!string.IsNullOrEmpty(this.Filename))
            {
                var logName = FileHelper.GetLogFile(this.Filename);
                return new RandomAccessFileFactory(logName, this.ReadOnly);
            }

            throw new ArgumentException("EngineSettings must have Filename or DataStream as data source");
            //return new StreamFactory(new MemoryStream(), string.Empty);
        }

        /// <summary>
        /// Create new IStreamFactory for temporary file (sort)
        /// </summary>
        internal IStreamFactory CreateTempFactory()
        {
            if (this.TempStream != null)
            {
                return new StreamFactory(this.TempStream, string.Empty);
            }
            else if (this.Filename == ":memory:")
            {
                return new StreamFactory(new MemoryStream(), string.Empty);
            }
            else if (this.Filename == ":temp:")
            {
                return new StreamFactory(new TempStream(), string.Empty);
            }
            else if (!string.IsNullOrEmpty(this.Filename))
            {
                var tempName = FileHelper.GetTempFile(this.Filename);

                return new FileStreamFactory(tempName, string.Empty, false, true);
            }

            return new StreamFactory(new TempStream(), string.Empty);
        }

        #region compatibility
        // TODO: kept for compatibility reasons during migration

        /// <summary>
        /// Create new IStreamFactory for datafile
        /// </summary>
        internal IStreamFactory CreateDataFactory()
        {
            if (this.Filename == ":memory:")
            {
                return new StreamFactory(new MemoryStream(), string.Empty);
            }

            if (this.Filename == ":temp:")
            {
                return new StreamFactory(new TempStream(), string.Empty);
            }

            if (!string.IsNullOrEmpty(this.Filename))
            {
                return new FileStreamFactory(this.Filename, string.Empty, this.ReadOnly, false);
            }

            throw new ArgumentException("EngineSettings must have Filename or DataStream as data source");
        }

        /// <summary>
        /// Create new IStreamFactory for logfile
        /// </summary>
        internal IStreamFactory CreateLogFactory()
        {
            if (this.Filename == ":memory:")
            {
                return new StreamFactory(new MemoryStream(), string.Empty);
            }

            if (this.Filename == ":temp:")
            {
                return new StreamFactory(new TempStream(), string.Empty);
            }

            if (!string.IsNullOrEmpty(this.Filename))
            {
                var logName = FileHelper.GetLogFile(this.Filename);

                return new FileStreamFactory(logName, string.Empty, this.ReadOnly, false);
            }

            return new StreamFactory(new MemoryStream(), string.Empty);
        }

        #endregion
    }
}