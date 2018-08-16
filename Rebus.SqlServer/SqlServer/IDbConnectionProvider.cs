using System.Data.SqlClient;
using System.Threading.Tasks;

namespace Rebus.SqlServer
{
    /// <summary>
    /// SQL Server database connection provider that allows for easily changing how the current <see cref="SqlConnection"/> is obtained,
    /// possibly also changing how transactions are handled
    /// </summary>
    public interface IDbConnectionProvider
    {
        /// <summary>
        /// Gets a wrapper with the current <see cref="SqlConnection"/> inside
        /// </summary>
        Task<IDbConnection> GetConnection();

        /// <summary>
        /// Gets a wrapper with the current <see cref="SqlConnection"/> inside
        /// If the connection could not be established retrys will be performed according to [<paramref name="retryCount"/>].
        /// </summary>
        Task<IDbConnection> GetConnection( int retryCount );
    }
}