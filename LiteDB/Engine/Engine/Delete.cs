﻿using System;
using System.Collections.Generic;
using System.Linq;

namespace LiteDB
{
    public partial class LiteEngine
    {
        /// <summary>
        /// Implements delete based on IDs enumerable
        /// </summary>
        public int Delete(string collection, IEnumerable<BsonValue> ids)
        {
            if (collection.IsNullOrWhiteSpace()) throw new ArgumentNullException(nameof(collection));
            if (ids == null) throw new ArgumentNullException(nameof(ids));

            using (var trans = this.BeginTrans())
            {
                var col = trans.Collection.Get(collection);

                if (col == null) return 0;

                var count = 0;

                // lock collection
                trans.WriteLock(collection);

                var query = new QueryIn("_id", ids);

                foreach (var pkNode in query.Run(col, trans.Indexer))
                {
                    // get all indexes nodes from this data block
                    var allNodes = trans.Indexer.GetNodeList(pkNode, true).ToArray();

                    // lets remove all indexes that point to this in dataBlock
                    foreach (var linkNode in allNodes)
                    {
                        var index = col.Indexes[linkNode.Slot];

                        trans.Indexer.Delete(index, linkNode.Position);
                    }

                    // remove object data
                    trans.Data.Delete(col, pkNode.DataBlock);

                    count++;
                }

                // persist changes
                trans.Commit();

                return count;
            }
        }
    }
}