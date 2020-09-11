using System.Data.SqlClient;

namespace Bloomberglp.Blpapi.BMpipeApp
{
  public interface ISqlHelper
  {
    SqlConnection GetConnection();
  }
}
