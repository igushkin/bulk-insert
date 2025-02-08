using System.Data;
using Microsoft.Data.SqlClient;

namespace BulkUpload
{
    class BulkDataLoader : IDisposable
    {
        private readonly SqlConnection sqlConnection;
        private readonly string tmpTableName;
        private readonly int minWordFreq;

        public BulkDataLoader(SqlConnection connection, string tmpTableName, int minWordFreq)
        {
            this.sqlConnection = connection;
            this.tmpTableName = tmpTableName;
            this.minWordFreq = minWordFreq;
        }

        public void BulkInsert(DataTable data)
        {
            using var sqlBulkCopy = new SqlBulkCopy(sqlConnection);
            sqlBulkCopy.DestinationTableName = tmpTableName;
            sqlBulkCopy.WriteToServer(data);
        }

        public void Merge(string targetTable)
        {
            var query = $@"
                MERGE INTO {targetTable} AS target
                USING ( 
                select word, count(*)
                FROM {tmpTableName}
                group by word
                having count(*) >= {minWordFreq}) AS source (word, freq)
                ON target.word = source.word
                WHEN MATCHED THEN
                UPDATE SET target.freq = target.freq + source.freq
                WHEN NOT MATCHED THEN
                INSERT (word, freq)
                VALUES (source.word, source.freq);
                ";

            using var cmd = new SqlCommand(query, sqlConnection);
            cmd.ExecuteNonQuery();
        }

        public void Dispose()
        {
            sqlConnection.Dispose();
        }
    }
}