using System.Data;
using System.Diagnostics;
using BulkUpload;
using Microsoft.Data.SqlClient;

string connectionString = @"Server=MARK\SQLEXPRESS;Database=local;trusted_connection=true;Encrypt=false";
string fileName = "data.txt";
string tmpTableName = "#words";
string targetTableName = "words";

int minWordLength = 3;
int maxWordLength = 20;
int minWordFreq = 4;

using (var reader = new CustomFileReader(fileName, minWordLength, maxWordLength))
using (var connection = new SqlConnection(connectionString))
using (var loader = new BulkDataLoader(connection, tmpTableName, minWordFreq))
{
    try
    {
        Stopwatch stopWatch = new Stopwatch();
        stopWatch.Start();
        connection.Open();

        createTmpTable(connection, tmpTableName, maxWordLength);
        createTargetTable(connection, targetTableName, maxWordLength);

        var data = new DataTable();
        data.Columns.Add("word");

        while (true)
        {
            var str = reader.ReadNextWord();
            if (str == null) break;
            if (data.Rows.Count == 10_000)
            {
                loader.BulkInsert(data);
                data.Clear();
            }
            Console.WriteLine(str);
            data.Rows.Add(str);
        }

        loader.BulkInsert(data);
        loader.Merge(targetTableName);

        stopWatch.Stop();
        Console.WriteLine(stopWatch.Elapsed.ToString());
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Ошибка подключения: {ex.Message}");
    }
}


static void createTargetTable(SqlConnection sqlConnection, string tableName, int maxWordLength) {
    var query = $@"
        IF OBJECT_ID('{tableName}', 'U') IS NULL
        CREATE TABLE {tableName} 
        (
            word nchar({maxWordLength}) PRIMARY KEY,
            freq INT not null
        )";

    using var cmd = new SqlCommand(query, sqlConnection);
    cmd.ExecuteNonQuery();
}

static void createTmpTable(SqlConnection sqlConnection, string tableName, int maxWordLength)
{
    var query = $@"
                CREATE TABLE {tableName}
                (word NVARCHAR({maxWordLength}))
                ";
    using var cmd = new SqlCommand(query, sqlConnection);
    cmd.ExecuteNonQuery();
}