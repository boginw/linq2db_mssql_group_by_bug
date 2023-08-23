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
    [Column(Name = "No_", Length = 20, IsPrimaryKey = true, CanBeNull = false)]
    public string No { get; set; } = string.Empty;

    [Column(Name = "Order No_", Length = 20, CanBeNull = false)]
    public string OrderNo { get; set; } = string.Empty;

    [Column(Name = "Portfolio Customer", CanBeNull = false)]
    public byte PortfolioCustomer { get; set; }
}

[Table(Name = "Purch_ Inv_ Line")]
public record NavPurchInvLine
{
    [Column(Name = "No_", Length = 20, CanBeNull = false)]
    public string No { get; set; } = string.Empty;

    [Column(Name = "Document No_", Length = 20, IsPrimaryKey = true, CanBeNull = false)]
    public string DocumentNo { get; set; } = string.Empty;
}

class TestHeader
{
    public required string No { get; init; }
    public required string OrderNo { get; init; }
    public bool Other { get; init; }
    public bool? IsActive { get; init; }
}

class TestLine
{
    public required string No { get; init; }
    public required string DocumentNo { get; init; }
    public bool? IsActive { get; init; }
}

class Program
{
    public static async Task Main()
    {
        await using var context = GetDataContext();

        var testLines =
            context.GetTable<NavPurchInvLine>()
                .Select(it => new TestLine
                {
                    No = it.No,
                    DocumentNo = it.DocumentNo,
                    IsActive = true,
                });

        var headersWithLines =
            context.GetTable<NavPurchInvHeader>()
                .Select(it => new TestHeader
                {
                    No = it.No,
                    OrderNo = it.OrderNo,
                    IsActive = true,
                    Other = Convert.ToBoolean(it.PortfolioCustomer)
                })
                .LeftJoin(
                    testLines,
                    (header, line) => header.No == line.DocumentNo,
                    (order, line) => new { order, line }
                )
                // .Where( ...conditions on the lines here... )
                .GroupBy(it => it.order)
                .Select(it => it.Key);

        Console.WriteLine(await headersWithLines.ToArrayAsync(CancellationToken.None));
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
            context.CreateTable<NavPurchInvLine>();
        }
        
        return context;
    }
}