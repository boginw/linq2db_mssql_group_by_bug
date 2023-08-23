using System.Data.Common;
using System.Data.SqlClient;
using LinqToDB;
using LinqToDB.Data;
using LinqToDB.DataProvider;
using LinqToDB.DataProvider.SQLite;
using LinqToDB.DataProvider.SqlServer;
using LinqToDB.Mapping;
using Microsoft.Data.Sqlite;

namespace linq2db_mssql_group_by_bug;

[Table(Name = "Purch_ Inv_ Header")]
public record NavPurchInvHeader
{
    [Column(Name = "Portfolio Customer", CanBeNull = false)]
    public byte PortfolioCustomer { get; set; }
}

class TestHeader
{
    public bool Other { get; init; }
    public bool? IsActive { get; init; }
}

class Program
{
    public static async Task Main()
    {
        await using var context = GetDataContext();
        
        var headers =
            context.GetTable<NavPurchInvHeader>()
                .Select(it => new TestHeader
                {
                    IsActive = true,
                    Other = Convert.ToBoolean(it.PortfolioCustomer)
                })
                .GroupBy(it => it)
                .Select(it => it.Key);

        Console.WriteLine(headers.ToString());
        Console.WriteLine(await headers.ToArrayAsync(CancellationToken.None));
    }

    private static DataConnection GetDataContext()
    {
        bool useSqlite = true;
        (DbConnection connection, IDataProvider provider) = useSqlite switch
        {
            false => (
                connection: (DbConnection)new SqlConnection(
                    "Data Source=----;Initial Catalog=-----;User ID=----;Password=----"),
                provider: SqlServerTools.GetDataProvider(SqlServerVersion.v2017)
            ),
            true => (
                connection: new SqliteConnection("Data Source=InMemoryDb;Mode=Memory;Cache=Shared"),
                provider: SQLiteTools.GetDataProvider()
            )
        };
        var options = new DataOptions().UseConnection(provider, connection, disposeConnection: true);
        var context = new DataConnection(options);

        if (useSqlite)
        {
            context.CreateTable<NavPurchInvHeader>();
        }
        
        return context;
    }
}
