using System;
using System.Configuration;
using System.Data.SqlClient;

namespace Bloomberglp.Blpapi.BMpipeApp
{
  public class SqlHelper : ISqlHelper
  {
    public SqlHelper()
    {
      if (ConfigurationManager.ConnectionStrings["ASSETSMGR"] == null)
        throw new Exception(String.Format("Can't find a connection string by the name \"{0}\"", "ASSETSMGR"));
      _connectionString = ConfigurationManager.ConnectionStrings["ASSETSMGR"].ConnectionString;
    }
    private readonly string _connectionString;

    public SqlConnection GetConnection()
    {
      return new SqlConnection(_connectionString);
    }

  }
}
