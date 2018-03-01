using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Logging;
using Rebus.SqlServer.Transport;
using Rebus.Tests.Contracts;
using Rebus.Threading;
using Rebus.Routing.TypeBased;

namespace Rebus.SqlServer.Tests.Integration
{
    [TestFixture]
    public class TestNumberOfSqlConnections : FixtureBase
    {
        [Test]
        public async Task CountTheConnections()
        {
            var activeConnections = new ConcurrentDictionary<int, object>();

            var bus = Configure.With(new BuiltinHandlerActivator())
                .Transport(t => t.Register(c =>
                {
                    var connectionProvider = new TestConnectionProvider(SqlTestHelper.ConnectionString, activeConnections);
                    var transport = new SqlServerTransport(connectionProvider, "RebusMessages", "bimse", c.Get<IRebusLoggerFactory>(), c.Get<IAsyncTaskFactory>());

                    transport.EnsureTableIsCreated();

                    return transport;
                }))
                .Start();

            using (new Timer(_ => Console.WriteLine("Active connections: {0}", activeConnections.Count), null, 0, 1000))
            {
                using (bus)
                {
                    await Task.Delay(TimeSpan.FromSeconds(5));
                }
            }
        }

        [Test]
        public async Task CanConnectWithRetryImpl()
        {
            var activeConnections = new ConcurrentDictionary<int, object>();

            var asyncDelegate = new AsyncTestDelegate( () =>
            {
                var bus = Configure.With( new BuiltinHandlerActivator() )
                    .Transport( t => t.Register( c =>
                    {
                        var connectionProvider = new TestConnectionProvider( SqlTestHelper.ConnectionString, activeConnections );
                        var transport = new SqlServerTransport( connectionProvider, "RebusMessages", "test-queue", c.Get<IRebusLoggerFactory>(), c.Get<IAsyncTaskFactory>() );

                        transport.EnsureTableIsCreated();

                        var failingConnectionString = "Data Source=.;Initial Catalog=Fail;Persist Security Info=True;";
                        var testedConnectionProvider = new TestConnectionProvider( failingConnectionString, activeConnections );
                        var testedTransport = new SqlServerTransport( testedConnectionProvider, "RebusMessages", "test-queue", c.Get<IRebusLoggerFactory>(), c.Get<IAsyncTaskFactory>() );

                        return testedTransport;
                    } ) )
                    .Routing( r => r.TypeBased().Map<object>( "test-queue" ) )
                    .Start();

                return bus.Send( new object() );
            } );

            Assert.CatchAsync( asyncDelegate );
        }

        class TestConnectionProvider : IDbConnectionProvider
        {
            static int _counter;

            readonly ConcurrentDictionary<int, object> _activeConnections;
            readonly IDbConnectionProvider _inner;

            public TestConnectionProvider(string connectionString, ConcurrentDictionary<int, object> activeConnections)
            {
                _activeConnections = activeConnections;
                _inner = new DbConnectionProvider(connectionString, new ConsoleLoggerFactory(true));
            }

            public async Task<IDbConnection> GetConnection()
            {
                return new Bimse(await _inner.GetConnection(), Interlocked.Increment(ref _counter), _activeConnections);
            }

            public async Task<IDbConnection> GetConnection( int retryCount )
            {
                return new Bimse( await _inner.GetConnection( 3 ), Interlocked.Increment( ref _counter ), _activeConnections );
            }

            class Bimse : IDbConnection
            {
                readonly IDbConnection _innerConnection;
                readonly ConcurrentDictionary<int, object> _activeConnections;
                readonly int _id;

                public Bimse(IDbConnection innerConnection, int id, ConcurrentDictionary<int, object> activeConnections)
                {
                    _innerConnection = innerConnection;
                    _id = id;
                    _activeConnections = activeConnections;
                    _activeConnections[id] = new object();
                }

                public SqlCommand CreateCommand()
                {
                    return _innerConnection.CreateCommand();
                }

                public IEnumerable<TableName> GetTableNames()
                {
                    return _innerConnection.GetTableNames();
                }

                public async Task Complete()
                {
                    await _innerConnection.Complete();
                }

                public IEnumerable<DbColumn> GetColumns(string schema, string dataTableName)
                {
                    return _innerConnection.GetColumns(schema, dataTableName);
                }

                public void Dispose()
                {
                    _innerConnection.Dispose();

                    object o;
                    _activeConnections.TryRemove(_id, out o);
                }
            }
        }
    }
}