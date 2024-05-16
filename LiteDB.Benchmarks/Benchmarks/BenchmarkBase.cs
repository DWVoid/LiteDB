using BenchmarkDotNet.Attributes;

namespace LiteDB.Benchmarks.Benchmarks
{
    public abstract class BenchmarkBase
    {
        // Insertion data size
        [Params(10, 50, 100, 500, 1000, 5000, 10000)]
        public int DatasetSize;

        public virtual string DatabasePath
        {
            get => Constants.DATABASE_NAME;
            set => throw new System.NotImplementedException();
        }

        public ConnectionString ConnectionString() => new ConnectionString(DatabasePath);

        protected ILiteDatabase DatabaseInstance { get; set; }
    }
}