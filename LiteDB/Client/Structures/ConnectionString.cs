using LiteDB.Engine;
using System;
using System.Collections.Generic;

namespace LiteDB;

/// <summary>
/// Manage ConnectionString to connect and create databases. Connection string are NameValue using Name1=Value1; Name2=Value2
/// </summary>
public class ConnectionString
{
    private readonly Dictionary<string, string> _values;

    /// <summary>
    /// "filename": Full path or relative path from DLL directory
    /// </summary>
    public string Filename { get; set; } = "";

    /// <summary>
    /// "readonly": Open datafile in readonly mode (default: false)
    /// </summary>
    public bool ReadOnly { get; set; } = false;

    /// <summary>
    /// "auto-rebuild": If last close database exception result a invalid data state, rebuild datafile on next open (default: false)
    /// </summary>
    public bool AutoRebuild { get; set; } = false;

    /// <summary>
    /// "collation": Set default collaction when database creation (default: "[CurrentCulture]/IgnoreCase")
    /// </summary>
    public Collation Collation { get; set; }

    /// <summary>
    /// Initialize empty connection string
    /// </summary>
    public ConnectionString()
    {
        _values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Initialize connection string parsing string in "key1=value1;key2=value2;...." format or only "filename" as default (when no ; char found)
    /// </summary>
    public ConnectionString(string connectionString)
        : this()
    {
        if (string.IsNullOrEmpty(connectionString)) throw new ArgumentNullException(nameof(connectionString));

        // create a dictionary from string name=value collection
        if (connectionString.Contains("="))
        {
            _values.ParseKeyValue(connectionString);
        }
        else
        {
            _values["filename"] = connectionString;
        }

        // setting values to properties
        this.Filename = _values.GetValue("filename", this.Filename).Trim();
        this.ReadOnly = _values.GetValue("readonly", this.ReadOnly);
        this.Collation = _values.ContainsKey("collation") ? new Collation(_values.GetValue<string>("collation")) : this.Collation;
        this.AutoRebuild = _values.GetValue("auto-rebuild", this.AutoRebuild);
    }

    /// <summary>
    /// Get value from parsed connection string. Returns null if not found
    /// </summary>
    public string this[string key] => _values.GetOrDefault(key);

    /// <summary>
    /// Create ILiteEngine instance according string connection parameters. For now, only Local/Shared are supported
    /// </summary>
    internal ILiteEngine CreateEngine()
    {
        var settings = new EngineSettings
        {
            Filename = this.Filename,
            ReadOnly = this.ReadOnly,
            Collation = this.Collation,
            AutoRebuild = this.AutoRebuild,
        };

        // create engine implementation as Connection Type
        return new LiteEngine(settings);
    }
}