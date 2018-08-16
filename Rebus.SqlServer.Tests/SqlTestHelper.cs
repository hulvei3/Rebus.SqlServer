﻿using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using Rebus.SqlServer.Tests.Extensions;
using Rebus.Exceptions;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Extensions;

namespace Rebus.SqlServer.Tests
{
    public class SqlTestHelper
    {
        static bool _databaseHasBeenInitialized;

        static string _connectionString;

        public static string ConnectionString
        {
            get
            {
                if (_connectionString != null)
                {
                    return _connectionString;
                }

                var databaseName = DatabaseName;

                if (!_databaseHasBeenInitialized)
                {
                    InitializeDatabase(databaseName);
                }

                Console.WriteLine("Using local SQL database {0}", databaseName);

                _connectionString = GetConnectionStringForDatabase(databaseName);

                return _connectionString;
            }
        }

        public static string DatabaseName => $"rebus2_test_{TestConfig.Suffix}".TrimEnd('_');

        public static void Execute(string sql)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = sql;
                    command.ExecuteNonQuery();
                }
            }
        }

        public static void DropTable(string tableName)
        {
            DropTable(TableName.Parse(tableName));
        }

        public static void DropTable(TableName tableName)
        {
            DropObject($"DROP TABLE {tableName.QualifiedName}", connection =>
            {
                var tableNames = connection.GetTableNames();

                return tableNames.Contains(tableName);
            });
        }

        public static void DropIndex(string tableName, string indexName)
        {
            DropObject($"DROP INDEX [{indexName}] ON [{tableName}]", connection =>
            {
                var indexNames = connection.GetIndexNames();

                return indexNames.Contains(indexName, StringComparer.OrdinalIgnoreCase);
            });
        }

        static void DropObject(string sqlCommand, Func<SqlConnection, bool> executeCriteria)
        {
            try
            {
                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();

                    var shouldExecute = executeCriteria(connection);
                    if (!shouldExecute) return;

                    try
                    {
                        using (var command = connection.CreateCommand())
                        {
                            command.CommandText = sqlCommand;
                            command.ExecuteNonQuery();
                        }
                    }
                    catch (SqlException exception)
                    {
                        if (exception.Number == SqlServerMagic.ObjectDoesNotExistOrNoPermission) return;

                        throw;
                    }
                }
            }
            catch (Exception exception)
            {
                DumpWho();

                throw new RebusApplicationException(exception, $"Could not execute '{sqlCommand}'");
            }
        }


        static void DumpWho()
        {
            try
            {
                Console.WriteLine("Trying to dump all active connections for db {0}...", DatabaseName);
                Console.WriteLine();

                var who = ExecSpWho()
                    .Where(kvp => kvp.ContainsKey("dbname"))
                    .Where(kvp => kvp["dbname"].Equals(DatabaseName, StringComparison.OrdinalIgnoreCase));

                Console.WriteLine(string.Join(Environment.NewLine,
                    who.Select(d => string.Join(", ", d.Select(kvp => $"{kvp.Key} = {kvp.Value}")))));

                Console.WriteLine();
            }
            catch (Exception exception)
            {
                Console.WriteLine("Could not execute sp_who: {0}", exception);
            }
        }

        public static IEnumerable<T> Query<T>(string query)
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();

                foreach (var result in connection.Query<T>(query))
                {
                    yield return result;
                }
            }
        }

        public static void DropAllTables()
        {
            var tableNames = GetTableNames();
            var troublesomeTables = new List<TableName>();

            foreach (var tableName in tableNames)
            {
                try
                {
                    DropTable(tableName);
                }
                catch
                {
                    // most likely because of a FK constraint...
                    troublesomeTables.Add(tableName);
                }
            }

            // try again - this time, at least we know the first level of child tables is gone, which is what's needed now... if multiple levels of FK references are introduced in the future, it might require more passes to do this
            foreach (var tableName in troublesomeTables)
            {
                DropTable(tableName);
            }
        }

        static List<TableName> GetTableNames()
        {
            var tableNames = new List<TableName>();

            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
select s.name as 'schema', t.name as 'table' from sys.tables t
	join sys.schemas s on s.schema_id = t.schema_id
";
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            tableNames.Add(new TableName((string)reader["schema"], (string)reader["table"]));
                        }
                    }
                }
            }

            return tableNames;
        }

        public static IEnumerable<IDictionary<string, string>> ExecSpWho()
        {
            using (var connection = new SqlConnection(ConnectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "sp_who;";

                    using (var reader = command.ExecuteReader())
                    {
                        var rows = new List<Dictionary<string, string>>();

                        while (reader.Read())
                        {
                            rows.Add(Enumerable.Range(0, reader.FieldCount)
                                .Select(field => new
                                {
                                    ColumnName = reader.GetName(field),
                                    Value = (reader.GetValue(field) ?? "").ToString().Trim()
                                })
                                .ToDictionary(a => a.ColumnName, a => a.Value));
                        }

                        return rows;
                    }
                }
            }
        }

        static void InitializeDatabase(string databaseName)
        {
            try
            {
                var masterConnectionString = GetConnectionStringForDatabase("master");

                using (var connection = new SqlConnection(masterConnectionString))
                {
                    connection.Open();

                    if (connection.GetDatabaseNames().Contains(databaseName, StringComparer.OrdinalIgnoreCase)) return;

                    Console.WriteLine("Creating database {0}", databaseName);

                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = $"CREATE DATABASE [{databaseName}]";
                        command.ExecuteNonQuery();
                    }
                }

                _databaseHasBeenInitialized = true;

            }
            catch (Exception exception)
            {
                throw new RebusApplicationException(exception, $"Could not initialize database '{databaseName}'");
            }
        }

        static string GetConnectionStringForDatabase(string databaseName)
        {
            return Environment.GetEnvironmentVariable("REBUS_SQLSERVER")
                   ?? $"server=.; database={databaseName}; trusted_connection=true;";
        }
    }
}