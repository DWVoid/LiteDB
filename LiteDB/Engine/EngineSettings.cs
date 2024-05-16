using System;
using System.IO;
using LiteDB.Storage;

namespace LiteDB.Engine;

/// <summary>
/// All engine settings used to starts new engine
/// </summary>
public class EngineSettings
{
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
    internal IRandomAccessFactory CreateTempFileFactory()
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
            var tempName = FileHelper.GetTempFile(this.Filename);
            return new RandomAccessFileFactory(tempName, false);
        }

        throw new ArgumentException("EngineSettings must have Filename or DataStream as data source");
        //return new StreamFactory(new TempStream(), string.Empty);
    }
}