#nullable enable
using System;
using System.Threading;
using System.Threading.Tasks;
using JasperFx.Events;
using JasperFx.Events.Aggregation;
using JasperFx.Events.Projections;
using Marten;
using Marten.Events;
using Marten.Testing.Harness;
using Npgsql;
using Shouldly;
using TenantPartitionedEventsTests.Fixtures;
using Weasel.Postgresql;
using Xunit;

namespace TenantPartitionedEventsTests.Admin;

/// <summary>
///     Under <c>UseTenantPartitionedEvents</c>, removing a tenant drops its <c>mt_streams</c> partition. A
///     <c>[NaturalKey]</c> aggregate's <c>mt_natural_key_*</c> table is not partitioned by tenant, and its foreign
///     key to <c>mt_streams(id, tenant_id)</c> cascades on DELETE, which a partition detach is not. So the
///     tenant's natural key rows have to go before its <c>mt_streams</c> partition does.
/// </summary>
public class removing_a_tenant_with_natural_keys: PartitionedStoreContext
{
    protected override string SchemaPrefix => "tp_rm_nk";

    protected override void ConfigureStore(StoreOptions opts)
    {
        opts.Projections.Snapshot<RemovalCase>(SnapshotLifecycle.Inline);
    }

    [Fact]
    public async Task removing_a_tenant_takes_its_natural_keys_and_leaves_another_tenants_alone()
    {
        await Store.Advanced.AddMartenManagedTenantsAsync(CancellationToken.None, "alpha", "beta");
        await openCaseAsync("alpha", "CASE-1");
        var betaCase = await openCaseAsync("beta", "CASE-1");

        await Store.Advanced.RemoveMartenManagedTenantsAsync(["alpha"], CancellationToken.None);

        (await tableExistsAsync("mt_streams_alpha")).ShouldBeFalse();
        (await countNaturalKeysAsync("alpha")).ShouldBe(0);
        (await countNaturalKeysAsync("beta")).ShouldBe(1, "removing alpha must leave beta's natural keys alone");

        await using var session = Store.LightweightSession("beta");
        var found = await session.Events.FetchLatest<RemovalCase, RemovalCaseNumber>(new RemovalCaseNumber("CASE-1"),
            TestContext.Current.CancellationToken);
        found.ShouldNotBeNull();
        found.Id.ShouldBe(betaCase);
    }

    [Fact]
    public async Task deleting_all_of_a_tenants_data_takes_its_natural_keys()
    {
        await Store.Advanced.AddMartenManagedTenantsAsync(CancellationToken.None, "alpha", "beta");
        await openCaseAsync("alpha", "CASE-1");
        await openCaseAsync("beta", "CASE-1");

        await Store.Advanced.DeleteAllTenantDataAsync("alpha", CancellationToken.None);

        (await countNaturalKeysAsync("alpha")).ShouldBe(0);
        (await countNaturalKeysAsync("beta")).ShouldBe(1);
    }

    private async Task<Guid> openCaseAsync(string tenantId, string number)
    {
        var streamId = Guid.NewGuid();
        await using var session = Store.LightweightSession(tenantId);
        session.Events.StartStream<RemovalCase>(streamId, new RemovalCaseOpened(streamId, number));
        await session.SaveChangesAsync(TestContext.Current.CancellationToken);
        return streamId;
    }

    private async Task<bool> tableExistsAsync(string tableName)
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = conn.CreateCommand("select to_regclass(:name) is not null");
        cmd.Parameters.AddWithValue("name", $"{Schema}.{tableName}");
        return (bool)(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
    }

    private async Task<long> countNaturalKeysAsync(string tenantId)
    {
        await using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = conn.CreateCommand(
            $"select count(*) from {Schema}.mt_natural_key_removalcase where tenant_id = :t");
        cmd.Parameters.AddWithValue("t", tenantId);
        return (long)(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
    }
}

public sealed record RemovalCaseNumber(string Value);

public sealed record RemovalCaseOpened(Guid CaseId, string Number);

public sealed record RemovalCase
{
    public Guid Id { get; set; }

    [NaturalKey]
    public RemovalCaseNumber Number { get; set; } = null!;

    [NaturalKeySource]
    public static RemovalCase Create(RemovalCaseOpened e) =>
        new() { Id = e.CaseId, Number = new RemovalCaseNumber(e.Number) };
}
