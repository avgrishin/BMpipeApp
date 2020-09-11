using System.Data.SqlClient;
using System;

namespace Bloomberglp.Blpapi.BMpipeApp
{
  public static class Extensions
  {
    public static SqlCommand Command(this SqlConnection connection, string sql)
    {
      var command = new SqlCommand(sql, connection);
      command.CommandTimeout = 600;
      return command;
    }

    public static SqlCommand AddParameter(this SqlCommand command, string parameterName, object value)
    {
      command.Parameters.AddWithValue(parameterName, value);
      return command;
    }

    public static SqlDataReader ReadAll(this SqlDataReader reader, Action<SqlDataReader> action)
    {
      while (reader.Read())
        action(reader);
      reader.Close();
      return reader;
    }

    public static void Using(this SqlConnection connection, Action<SqlConnection> action)
    {
      using (connection)
      {
        try
        {
          connection.Open();
          action(connection);
        }
        finally
        {
          connection.Close();
          connection.Dispose();
        }
      }
    }

  }
}
