using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Bloomberglp.Blpapi.BMpipeApp
{
  public class BMpipeApp
  {
    public static void Main(string[] args)
    {
      Console.WriteLine("Load Bloomberg data");
      //Regex rdt = new Regex("^(0[1-9]|[12][0-9]|3[01])[.](0[1-9]|1[012])[.](19|20)\\d\\d$");
      Regex rdt = new Regex("^\\d\\d[.]\\d\\d[.](19|20)\\d\\d$");
      BMpipeApp example = new BMpipeApp();
      DateTime dt = DateTime.Today.AddDays(-1);
      CultureInfo culture = CultureInfo.CreateSpecificCulture("ru-RU");

      for (int i = 0; i < args.Length; ++i)
      {
        if (rdt.IsMatch(args[i]))
        {
          dt = DateTime.Parse(args[i], culture);
        }
        if (string.Compare(args[i], "1", true) == 0)
        {
          System.Console.WriteLine("Step 1");
          example.run1();
        }
        else if (string.Compare(args[i], "2", true) == 0)
        {
          System.Console.WriteLine("Step 2");
          example.run();
        }
        else if (string.Compare(args[i], "3", true) == 0)
        {
          System.Console.WriteLine("Step 3");
          example.run3();
        }
        else if (string.Compare(args[i], "4", true) == 0)
        {
          System.Console.WriteLine("Step 4");
          example.run4(dt);
        }
        else if (string.Compare(args[i], "5", true) == 0)
        {
          System.Console.WriteLine("Step 5");
          example.run5(dt);
        }
        else if (string.Compare(args[i], "6", true) == 0)
        {
          System.Console.WriteLine("Step 6");
          example.run6();
        }
        else if (string.Compare(args[i], "7", true) == 0)
        {
          System.Console.WriteLine("Step 7");
          example.run7();
        }
        else if (string.Compare(args[i], "8", true) == 0)
        {
          System.Console.WriteLine("Step 8");
          example.run8();
        }
        else if (string.Compare(args[i], "9", true) == 0)
        {
          System.Console.WriteLine("Step 9");
          example.run9();
        }
        else if (string.Compare(args[i], "10", true) == 0)
        {
          System.Console.WriteLine("Step 10");
          example.run10(dt);
        }
        else if (string.Compare(args[i], "11", true) == 0)
        {
          System.Console.WriteLine("Step 11");
          example.run11();
        }
        else if (string.Compare(args[i], "13", true) == 0)
        {
          System.Console.WriteLine("Step 13");
          example.run13();
        }
        else if (string.Compare(args[i], "15", true) == 0)
        {
          System.Console.WriteLine("Step 15");
          example.run15();
        }
        else if (string.Compare(args[i], "16", true) == 0)
        {
          System.Console.WriteLine("Step 16");
          example.run16();
        }
        else if (string.Compare(args[i], "18", true) == 0)
        {
          System.Console.WriteLine("Step 18");
          example.run18();
        }
        else if (string.Compare(args[i], "20", true) == 0)
        {
          System.Console.WriteLine("Step 20");
          example.run20();
        }
      }
    }

    private void run()
    {
      SqlHelper _sqlHelper = new SqlHelper();
      var list = new List<Security>();
      string sql = "delete pSecFlow " +
          "select s.SecurityID, s.Number+' Corp' ISIN, 'SETTLE_DT='+convert(varchar, sd.FIRST_CPN_DT-1, 112) Override " +
          "from tSecurity s " +
          "join tempdb..pSecSettleDate sd on sd.SecurityID = s.SecurityID " +
          "where s.SecurityID in (select SecurityID from tSecuritySecurityGroup where SecurityGroupID in (9, 10, 15, 846)) " +
          "and (s.DateEnd > GETDATE() or (s.DateEnd = '19000101' and s.SecurityID > 98000)) " +
          "and s.Number <> '' ";
      string sqlu = "insert pSecFlow (SecurityID, FDate, Value1, Value2) values (@SecurityID, @FDate, @Value1, @Value2)";
      _sqlHelper.GetConnection().Using(c =>
      {
        c.Command(sql)
          .ExecuteReader()
          .ReadAll(r =>
          {
            list.Add(new Security() { SecurityID = r.GetInt32(0), Ticker = r.GetString(1), Override = r.IsDBNull(2) ? "" : r.GetString(2) });
          });
        Blpapi blpapi = new Blpapi();
        if (blpapi.Connect())
        {
          foreach (Security s in list)
          {
            System.Console.WriteLine(string.Format("Tiker={0} ID={1}", s.Ticker, s.SecurityID));
            blpapi.sendRefDataBulkRequest(s.SecurityID, s.Ticker, "DES_CASH_FLOW", s.Override, e =>
            {
              c.Command(sqlu)
                .AddParameter("@SecurityID", s.SecurityID)
                .AddParameter("@FDate", !e.IsNullValue(0) ? (object)e.GetElement(0).GetValueAsDate().ToSystemDateTime() : DBNull.Value)
                .AddParameter("@Value1", !e.IsNullValue(1) ? (object)e.GetElement(1).GetValueAsFloat64() : DBNull.Value)
                .AddParameter("@Value2", !e.IsNullValue(2) ? (object)e.GetElement(2).GetValueAsFloat64() : DBNull.Value)
                .ExecuteNonQuery();
            });
          }
        }
      });
    }

    private void run1()
    {
      SqlHelper _sqlHelper = new SqlHelper();
      var list = new List<int>();
      var slist = new List<string>();
      var flist = new List<string> { "FIRST_CPN_DT", "FIRST_SETTLE_DT", "ISSUE_DT" };
      string sql =
          "select s.SecurityID, rtrim(ltrim(s.Number))+' Corp' ISIN, '' Override " +
          "from tSecurity s " +
          "where s.SecurityID in (select SecurityID from tSecuritySecurityGroup where SecurityGroupID in (9, 10, 15, 846)) " +
          "and (s.DateEnd > GETDATE() or (s.DateEnd = '19000101' and s.SecurityID > 98000)) " +
          "and s.Number <> '' ";
      var sb = new StringBuilder();
      sb.AppendLine("if object_id ('tempdb..pSecSettleDate') is not null  drop table tempdb..pSecSettleDate create table tempdb..pSecSettleDate (SecurityID int primary key, ISIN varchar(50)");
      var sbu1 = new StringBuilder();
      var sbu2 = new StringBuilder();
      foreach (var fe in flist)
      {
        sb.AppendFormat(",{0} smalldatetime", fe);
        sbu1.AppendFormat(",{0}", fe);
        sbu2.AppendFormat(",@{0}", fe);
      }
      sb.Append(")");
      string sqlu = String.Format("insert tempdb..pSecSettleDate(SecurityID,ISIN{0}) values (@SecurityID,@ISIN{1})", sbu1, sbu2);
      //string sqlu = "insert pSecSettleDate (SecurityID, SDate) values (@SecurityID, @SDate)";
      _sqlHelper.GetConnection().Using(c =>
      {
        c.Command(sb.ToString()).ExecuteNonQuery();
        c.Command(sql)
          .ExecuteReader()
          .ReadAll(r =>
          {
            list.Add(r.GetInt32(0));
            slist.Add(r.GetString(1));
          });
        Blpapi blpapi = new Blpapi();
        if (blpapi.Connect())
        {
          blpapi.sendRefDataRequestList(slist, flist, "", e =>
          {
            string ticker = e.GetElementAsString("security");
            int rowIndex = e.GetElementAsInt32("sequenceNumber");
            if (e.HasElement("securityError"))
            {
              Element securityError = e.GetElement("securityError");
              System.Console.WriteLine(string.Format("Ticker \"{0}\" - responseError", ticker));
              return;
            }
            else
            {
              Element fields = e.GetElement("fieldData");
              SqlCommand cmd = c.Command(sqlu).AddParameter("@SecurityID", list[rowIndex]).AddParameter("@ISIN", ticker);
              foreach (var fe in flist)
              {
                cmd.AddParameter("@" + fe, (fields.HasElement(fe) ? (object)fields.GetElementAsString(fe) : DBNull.Value));
              }
              cmd.ExecuteNonQuery();
            }
          });
        }
      });
    }

    private void run3()
    {
      SqlHelper _sqlHelper = new SqlHelper();
      var list = new List<int>();
      var slist = new List<string>();
      var flist = new List<string> { "RTG_MOODY", "RTG_FITCH", "RTG_SP", "RTG_SP_OUTLOOK", "RTG_SP_LT_FC_ISSUER_CREDIT", "RTG_SP_LT_LC_ISSUER_CREDIT", "RTG_FITCH_OUTLOOK", "RTG_FITCH_LT_ISSUER_DEFAULT", "RTG_FITCH_SHORT_TERM", "RTG_MDY_OUTLOOK", "RTG_MDY_FC_CURR_ISSUER_RATING", "RTG_MDY_LC_CURR_ISSUER_RATING", "RTG_MDY_SEN_UNSECURED_DEBT", "RTG_EXPERT_RA_ISSUER_CRDT_RTG", "RTG_FITCH_LT_FC_DEBT", "RTG_FITCH_LT_LC_DEBT", "RTG_FITCH_ST_LC_ISSUER_DEFAULT", "RTG_MOODY_LONG_TERM", "RTG_MDY_LT_CORP_FAMILY", "RTG_FITCH_LONG", "RTG_FITCH_LT_LC_ISSUER_DEFAULT" };
      var sb = new StringBuilder();
      sb.AppendLine("if object_id ('tempdb..pBloomRating') is not null  drop table tempdb..pBloomRating create table tempdb..pBloomRating (SecurityID int primary key, ISIN varchar(50)");
      var sbu1 = new StringBuilder();
      var sbu2 = new StringBuilder();
      foreach (var fe in flist)
      {
        sb.AppendFormat(",{0} varchar(50)", fe);
        sbu1.AppendFormat(",{0}", fe);
        sbu2.AppendFormat(",@{0}", fe);
      }
      sb.Append(")");
      string sqlu = String.Format("insert tempdb..pBloomRating(SecurityID,ISIN{0}) values (@SecurityID,@ISIN{1})", sbu1, sbu2);
      _sqlHelper.GetConnection().Using(c =>
      {
        c.Command(sb.ToString()).ExecuteNonQuery();
        var cmd1 = c.Command("exec up_avgGetBlmRatingSec null");
        cmd1.CommandTimeout = 600;
        cmd1.ExecuteReader()
        .ReadAll(r =>
        {
          list.Add(r.GetInt32(0));
          slist.Add(r.GetString(1));
          System.Console.WriteLine(string.Format("Tiker={0}", r.GetString(1)));
        });

        Blpapi blpapi = new Blpapi();
        if (blpapi.Connect())
        {
          blpapi.sendRefDataRequestList(slist, flist, "", e =>
          {
            string ticker = e.GetElementAsString("security");
            int rowIndex = e.GetElementAsInt32("sequenceNumber");
            if (e.HasElement("securityError"))
            {
              Element securityError = e.GetElement("securityError");
              System.Console.WriteLine(string.Format("Ticker \"{0}\" - responseError", ticker));
              SqlCommand cmd = c.Command(sqlu)
                .AddParameter("@SecurityID", list[rowIndex])
                .AddParameter("@ISIN", ticker);
              foreach (var fe in flist)
              {
                cmd.AddParameter("@" + fe, "Err");
              }
              cmd.ExecuteNonQuery();
            }
            else
            {
              Element fields = e.GetElement("fieldData");
              System.Console.WriteLine(string.Format("Ticker \"{0}\"", ticker));
              SqlCommand cmd = c.Command(sqlu)
                .AddParameter("@SecurityID", list[rowIndex])
                .AddParameter("@ISIN", ticker);
              foreach (var fe in flist)
              {
                cmd.AddParameter("@" + fe, (fields.HasElement(fe) ? (object)fields.GetElementAsString(fe) : DBNull.Value));
              }
              cmd.ExecuteNonQuery();
            }
          });
        }
      });
    }

    private void run4(DateTime dt)
    {
      SqlHelper _sqlHelper = new SqlHelper();
      var list = new List<int>();
      var slist = new List<string>();
      var flist = new List<string> { "CRNCY", "CPN_TYP", "DAY_CNT_DES", "CPN_FREQ", "NXT_CALL_DT", "ANNOUNCE_DT", "INT_ACC", "INT_ACC_DT", "FIRST_SETTLE_DT", "AMT_ISSUED", "PAR_AMT", "MARKET_STATUS", "NAME", /*"GICS_SECTOR_NAME", "GICS_SECTOR",*/ "ICB_SUPERSECTOR_NAME", "ICB_SUPERSECTOR_NUM", "GICS_INDUSTRY_NAME", "GICS_INDUSTRY", "ADR_SH_PER_ADR", "ADR_UNDL_TICKER", "ID_ISIN", "SECURITY_NAME", "SHORT_NAME", "id_sedol1", "MATURITY", "CBBT_PX_BID", "CBBT_PX_ASK", "EXCH_CODE", "DVD_CRNCY", "VOLUME_AVG_10D", "EQY_PRIM_EXCH_SHRT", "EQY_PRIM_SECURITY_COMP_EXCH", "CNTRY_OF_RISK", "CNTRY_OF_DOMICILE", "EQY_PRIM_SECURITY_PRIM_EXCH" };
      var sb = new StringBuilder();
      sb.AppendLine("if object_id ('tempdb..pBloomRates') is not null  drop table tempdb..pBloomRates create table tempdb..pBloomRates (SecurityID int, ISIN varchar(50), Date smalldatetime");
      var sbu1 = new StringBuilder();
      var sbu2 = new StringBuilder();
      foreach (var fe in flist)
      {
        sb.AppendFormat(",{0} varchar(50)", fe);
        sbu1.AppendFormat(",{0}", fe);
        sbu2.AppendFormat(",@{0}", fe);
      }
      sb.Append(",primary key(SecurityID, ISIN))");
      string sqlu = String.Format("insert tempdb..pBloomRates(SecurityID,ISIN,Date{0}) values (@SecurityID,@ISIN,@Date{1})", sbu1, sbu2);
      _sqlHelper.GetConnection().Using(c =>
      {
        c.Command(sb.ToString()).ExecuteNonQuery();
        c.Command("exec up_avgGetBlmRateSec")
          .ExecuteReader()
          .ReadAll(r =>
          {
            if (!r.GetString(3).Contains("BGN"))
            {
              list.Add(r.GetInt32(0));
              slist.Add(r.GetString(1));
            }
          });

        Blpapi blpapi = new Blpapi();
        if (blpapi.Connect())
        {
          //DateTime dt = DateTime.Today.AddDays(DateTime.Today.DayOfWeek == DayOfWeek.Sunday ? -2 : DateTime.Today.DayOfWeek == DayOfWeek.Monday ? -3 : -1);
          //DateTime dt = DateTime.Today.AddDays(-1);
          blpapi.sendRefDataRequestList(slist, flist,
            string.Format("SETTLE_DT={0:yyyyMMdd}", dt),
            e =>
            {
              string ticker = e.GetElementAsString("security");
              int rowIndex = e.GetElementAsInt32("sequenceNumber");
              if (e.HasElement("securityError"))
              {
                Element securityError = e.GetElement("securityError");
                System.Console.WriteLine(string.Format("Ticker \"{0}\" - responseError", ticker));
                SqlCommand cmd = c.Command(sqlu)
                  .AddParameter("@SecurityID", list[rowIndex])
                  .AddParameter("@ISIN", ticker)
                  .AddParameter("@Date", dt);
                foreach (var fe in flist)
                {
                  cmd.AddParameter("@" + fe, "Err");
                }
                cmd.ExecuteNonQuery();
              }
              else
              {
                Element fields = e.GetElement("fieldData");
                System.Console.WriteLine(string.Format("Ticker \"{0}\"", ticker));
                SqlCommand cmd = c.Command(sqlu)
                  .AddParameter("@SecurityID", list[rowIndex])
                  .AddParameter("@ISIN", ticker)
                  .AddParameter("@Date", dt);
                foreach (var fe in flist)
                {
                  cmd.AddParameter("@" + fe, (fields.HasElement(fe) ? (object)fields.GetElementAsString(fe) : DBNull.Value));
                }
                cmd.ExecuteNonQuery();
              }
            });
        }
      });
    }

    private void run5(DateTime dt)
    {
      SqlHelper _sqlHelper = new SqlHelper();
      var list = new List<Tuple<int, string>>();
      var slist = new List<string>();

      var sl = new List<Tuple<int, string, string, string>>();

      var flist = new List<string> { "PX_LAST", "HIGH", "LOW", "VOLUME", "PX_OPEN", "BID", "ASK", "TURNOVER", "NUM_TRADES" };
      var sb = new StringBuilder();
      sb.AppendLine("if object_id ('tempdb..pBloomRates1') is not null  drop table tempdb..pBloomRates1 create table tempdb..pBloomRates1 (SecurityID int, ISIN varchar(50), Date smalldatetime, PCS varchar(50)");
      var sbu1 = new StringBuilder();
      var sbu2 = new StringBuilder();
      foreach (var fe in flist)
      {
        sb.AppendFormat(",{0} varchar(50)", fe);
        sbu1.AppendFormat(",{0}", fe);
        sbu2.AppendFormat(",@{0}", fe);
      }
      sb.Append(", primary key (SecurityID, ISIN, Date, PCS))");
      string sqlu = String.Format("insert tempdb..pBloomRates1(SecurityID,ISIN,Date,PCS{0}) values (@SecurityID,@ISIN,@Date,@PCS{1})", sbu1, sbu2);
      _sqlHelper.GetConnection().Using(c =>
      {
        c.Command(sb.ToString()).ExecuteNonQuery();
        c.Command("exec up_avgGetBlmRateSec")
          .ExecuteReader()
          .ReadAll(r =>
          {
            //list.Add(r.GetInt32(0));
            //slist.Add(r.GetString(1));
            sl.Add(Tuple.Create(r.GetInt32(0), r.GetString(1), r.GetString(2), r.GetString(3)));
          });
        foreach (var val in sl.Select(p => p.Item3).Distinct())
        {
          list = sl.Where(p => p.Item3 == val).Select(p => Tuple.Create(p.Item1, p.Item4)).ToList();
          slist = sl.Where(p => p.Item3 == val).Select(p => p.Item2 + (string.IsNullOrEmpty(p.Item4) ? "" : "@" + p.Item4)).ToList();

          Blpapi blpapi = new Blpapi();
          if (blpapi.Connect())
          {
            //DateTime de = DateTime.Today.AddDays(DateTime.Today.DayOfWeek == DayOfWeek.Sunday ? -2 : DateTime.Today.DayOfWeek == DayOfWeek.Monday ? -3 : -1);
            //DateTime db = de.AddDays(de.DayOfWeek == DayOfWeek.Sunday ? -3 : de.DayOfWeek == DayOfWeek.Monday ? -4 : -2);
            DateTime de = dt;
            DateTime db = de.AddDays(de.DayOfWeek == DayOfWeek.Sunday ? -3 : de.DayOfWeek == DayOfWeek.Monday ? -4 : -2);

            blpapi.sendHistDataRequestList(slist, flist, "", val, db, de, e =>
            {
              string ticker = e.GetElementAsString("security");
              System.Console.WriteLine(string.Format("Ticker \"{0}\"", ticker));
              int rowIndex = e.GetElementAsInt32("sequenceNumber");
              if (e.HasElement("securityError"))
              {
                Element securityError = e.GetElement("securityError");
                System.Console.WriteLine(string.Format("Ticker \"{0}\" - responseError", ticker));
                SqlCommand cmd = c.Command(sqlu)
                  .AddParameter("@SecurityID", list[rowIndex].Item1)
                  .AddParameter("@ISIN", ticker)
                  .AddParameter("@Date", DateTime.Today)
                  .AddParameter("@PCS", list[rowIndex].Item2);
                foreach (var fe in flist)
                {
                  cmd.AddParameter("@" + fe, "Err");
                }
                cmd.ExecuteNonQuery();
              }
              else
              {
                Element fieldData = e.GetElement("fieldData");
                for (int j = 0; j < fieldData.NumValues; j++)
                {
                  Element element = fieldData.GetValueAsElement(j);
                  DateTime date = element.GetElementAsDatetime("date").ToSystemDateTime();
                  SqlCommand cmd = c.Command(sqlu)
                    .AddParameter("@SecurityID", list[rowIndex].Item1)
                    .AddParameter("@ISIN", ticker)
                    .AddParameter("@Date", date)
                    .AddParameter("@PCS", list[rowIndex].Item2);
                  foreach (var fe in flist)
                  {
                    cmd.AddParameter("@" + fe, (element.HasElement(fe) ? (object)element.GetElementAsString(fe) : DBNull.Value));
                  }
                  cmd.ExecuteNonQuery();
                  //System.Console.WriteLine(string.Format("{0} {1:dd.MM.yyyy} {2} {3}", ticker, date, element.HasElement("VOLUME") ? element.GetElementAsString("VOLUME") : "", element.HasElement("PX_LAST") ? element.GetElementAsString("PX_LAST") : ""));
                }
              }
            });
          }
        }
      });
    }

    private void run6()
    {
      SqlHelper _sqlHelper = new SqlHelper();
      var list = new List<int>();
      //var slist = new List<string>();
      var flist = new List<string> { "PX_LAST" };
      var sb = new StringBuilder();
      sb.AppendLine("if object_id ('tempdb..pBloomIndex') is not null  drop table tempdb..pBloomIndex create table tempdb..pBloomIndex (IndexID varchar(50), Date smalldatetime");
      var sbu1 = new StringBuilder();
      var sbu2 = new StringBuilder();
      foreach (var fe in flist)
      {
        sb.AppendFormat(",{0} varchar(50)", fe);
        sbu1.AppendFormat(",{0}", fe);
        sbu2.AppendFormat(",@{0}", fe);
      }
      sb.Append(", primary key (IndexID, Date))");
      string sqlu = String.Format("insert tempdb..pBloomIndex(IndexID,Date{0}) values (@IndexID,@Date{1})", sbu1, sbu2);
      _sqlHelper.GetConnection().Using(c =>
      {
        c.Command(sb.ToString()).ExecuteNonQuery();
        Blpapi blpapi = new Blpapi();
        if (blpapi.Connect())
        {
          /*"JPEMRUS INDEX",*/
          /*"RRSWM1 CMPN CURNCY", "RRSWM2 CMPN CURNCY", "RRSWM3 CMPN CURNCY", "RRSWM4 CMPN CURNCY", "RRSWM5 CMPN CURNCY", "RRSWM7 CMPN CURNCY", "RRSWM10 CMPN CURNCY", "RRSWM15 CMPN CURNCY",*/
          /*"MOSKON INDEX", "MOSKP1 INDEX", "MOSKP3 INDEX", "MOSKP6 INDEX", "MOSK1W INDEX",*/
          /*"RTSVX INDEX", "SGFS INDEX",*/
          /*"JCMBRU INDEX",*/
          /*"EURRUB BGN CURNCY", "GBPUSD BGN CURNCY", "CADUSD BGN CURNCY", "HKDUSD BGN CURNCY",*/
          /*"GSIN Index",*/
          blpapi.sendHistDataRequestList(new List<string> { "US0001M INDEX", "US0002M INDEX", "US0003M INDEX", "US0004M INDEX", "US0005M INDEX", "US0006M INDEX", "US0009M INDEX", "US0012M INDEX", "MXWO Index", "MXWO0MM Index", "MXWO0EN Index", "MXWO0FN Index", "MXWO0IT Index", "MXWO0TC Index", "DGLSO INDEX", "EMBSO INDEX", "VWOSO INDEX", "RWOSO INDEX", "DBCSO INDEX", "LQDSO INDEX", "IYFSO INDEX", "PNQISO INDEX", "DSISO INDEX", "IEZSO INDEX", "IEOSO INDEX", "SPX INDEX", "CCMP INDEX", "NDX INDEX", "UKX INDEX", "SPTSX INDEX", "XAU CURNCY", "USDRUB BGN CURNCY", "FCNTX US Equity", "DODGX US Equity", "FXAIX US Equity", "TRBCX US Equity", "FBGRX US Equity", "AGTHX US Equity", "EGFIX US Equity", "OAKMX US Equity", "SGENX US Equity", "POLIX US Equity", "SEQUX US Equity", "PFPFX US Equity", "LEIFX US Equity", "ABVAX US Equity", "SCAUX US Equity", "LLPFX US Equity",
          "SXXP Index", "CO1 Comdty", "RUONIA Index", "DXY Curncy", "MXEF Index", "CRB METL Index", "EURUSD CURNCY", "MOSKP3 INDEX", "GLTR US Equity","CRUSS1U5 Index", "CMEX1U5 Index", "CBRZ1U5 Index", "CTURK1U5 Index", "RRSWM1 CMPN CURNCY", "RRSWM3 CMPN CURNCY", "RRSWM5 CMPN CURNCY", "RRSWM7 CMPN CURNCY", "RRSWM10 CMPN CURNCY", "MOSKON INDEX", "LG30TRUU INDEX", "RPRMGOLD INDEX" }, flist, "", "", DateTime.Today.AddDays(-10), DateTime.Today, e =>
          //blpapi.sendHistDataRequestList(new List<string> { "CRUSS1U5 Index", "CMEX1U5 Index", "CBRZ1U5 Index", "CTURK1U5 Index" }, flist, "", "", DateTime.Today.AddYears(-10), DateTime.Today, e =>
          {
            string ticker = e.GetElementAsString("security");
            System.Console.WriteLine(string.Format("Ticker \"{0}\"", ticker));
            int rowIndex = e.GetElementAsInt32("sequenceNumber");
            if (e.HasElement("securityError"))
            {
              Element securityError = e.GetElement("securityError");
              System.Console.WriteLine(string.Format("Ticker \"{0}\" - responseError", ticker));
              SqlCommand cmd = c.Command(sqlu)
                  .AddParameter("@IndexID", ticker)
                  .AddParameter("@Date", DateTime.Today);
              foreach (var fe in flist)
              {
                cmd.AddParameter("@" + fe, "Err");
              }
              cmd.ExecuteNonQuery();
            }
            else
            {
              Element fieldData = e.GetElement("fieldData");
              for (int j = 0; j < fieldData.NumValues; j++)
              {
                Element element = fieldData.GetValueAsElement(j);
                DateTime date = element.GetElementAsDatetime("date").ToSystemDateTime();
                SqlCommand cmd = c.Command(sqlu)
                    .AddParameter("@IndexID", ticker)
                    .AddParameter("@Date", date);
                foreach (var fe in flist)
                {
                  cmd.AddParameter("@" + fe, (element.HasElement(fe) ? (object)element.GetElementAsString(fe) : DBNull.Value));
                }
                cmd.ExecuteNonQuery();
                System.Console.WriteLine(string.Format("{0} {1:dd.MM.yyyy} {2}", ticker, date, element.HasElement("PX_LAST") ? element.GetElementAsString("PX_LAST") : ""));
              }

            }
          });
        }
      });
    }

    private void run7()
    {
      SqlHelper _sqlHelper = new SqlHelper();
      var flist = new List<string> { "CRNCY", "INT_ACC", "ID_ISIN", "SECURITY_NAME", "SHORT_NAME", "MATURITY" };
      var sb = new StringBuilder();
      sb.AppendLine("if object_id ('tempdb..pBloomRates7') is not null  drop table tempdb..pBloomRates7 create table tempdb..pBloomRates7 (SecurityID int, ISIN varchar(50), Date smalldatetime");
      var sbu1 = new StringBuilder();
      var sbu2 = new StringBuilder();
      foreach (var fe in flist)
      {
        sb.AppendFormat(",{0} varchar(50)", fe);
        sbu1.AppendFormat(",{0}", fe);
        sbu2.AppendFormat(",@{0}", fe);
      }
      sb.Append(", primary key (SecurityID, ISIN, Date))");
      string sqlu = String.Format("insert tempdb..pBloomRates7(SecurityID,ISIN,Date{0}) values (@SecurityID,@ISIN,@Date{1})", sbu1, sbu2);
      Blpapi blpapi = new Blpapi();
      if (blpapi.Connect())
      {
        _sqlHelper.GetConnection().Using(c =>
        {
          c.Command(sb.ToString()).ExecuteNonQuery();
          c.Command("exec up_avgGetBlmRateSec1")
            .ExecuteReader()
            .ReadAll(r =>
            {
              System.Console.WriteLine("{0} {1:dd-MM-yy}", r.GetString(1), r.GetDateTime(2));
              blpapi.sendRefDataRequestList(new List<string> { r.GetString(1) }, flist, string.Format("SETTLE_DT={0:yyyyMMdd};EQY_FUND_CRNCY=USD", r.GetDateTime(2)), e =>
              {
                if (e.HasElement("securityError"))
                {
                  Element securityError = e.GetElement("securityError");
                  System.Console.WriteLine(string.Format("Ticker \"{0}\" - responseError", r.GetString(1)));
                  SqlCommand cmd = c.Command(sqlu).AddParameter("@SecurityID", r.GetInt32(0)).AddParameter("@ISIN", r.GetString(1)).AddParameter("@Date", r.GetDateTime(2));
                  foreach (var fe in flist) cmd.AddParameter("@" + fe, "Err");
                  cmd.ExecuteNonQuery();
                }
                else
                {
                  System.Console.WriteLine(string.Format("Ticker \"{0}\"", e.GetElementAsString("security")));
                  Element fields = e.GetElement("fieldData");
                  SqlCommand cmd = c.Command(sqlu)
                    .AddParameter("@SecurityID", r.GetInt32(0))
                    .AddParameter("@ISIN", r.GetString(1))
                    .AddParameter("@Date", r.GetDateTime(2));
                  foreach (var fe in flist)
                  {
                    cmd.AddParameter("@" + fe, (fields.HasElement(fe) ? (object)fields.GetElementAsString(fe) : DBNull.Value));
                  }
                  cmd.ExecuteNonQuery();
                }

              });
            });
        });
      }
    }

    private void run8()
    {
      SqlHelper _sqlHelper = new SqlHelper();
      var flist = new List<string> { "PX_LAST", "HIGH", "LOW", "VOLUME", "PX_OPEN", "BID", "ASK", "TURNOVER", "NUM_TRADES" };
      var sb = new StringBuilder();
      sb.AppendLine("if object_id ('tempdb..pBloomRates8') is not null  drop table tempdb..pBloomRates8 create table tempdb..pBloomRates8 (SecurityID int, ISIN varchar(50), Date smalldatetime, PCS varchar(50)");
      var sbu1 = new StringBuilder();
      var sbu2 = new StringBuilder();
      foreach (var fe in flist)
      {
        sb.AppendFormat(",{0} varchar(50)", fe);
        sbu1.AppendFormat(",{0}", fe);
        sbu2.AppendFormat(",@{0}", fe);
      }
      sb.Append(", primary key (SecurityID, ISIN, Date, PCS))");
      string sqlu = String.Format("insert tempdb..pBloomRates8(SecurityID,ISIN,Date,PCS{0}) values (@SecurityID,@ISIN,@Date,@PCS{1})", sbu1, sbu2);
      Blpapi blpapi = new Blpapi();
      if (blpapi.Connect())
      {
        _sqlHelper.GetConnection().Using(c =>
        {
          c.Command(sb.ToString()).ExecuteNonQuery();
          c.Command("exec up_avgGetBlmRateSec2")
            .ExecuteReader()
            .ReadAll(r =>
            {
              System.Console.WriteLine("{0} {1:dd-MM-yy} {2:dd-MM-yy}", r.GetString(1) + (string.IsNullOrEmpty(r.GetString(5)) ? "" : "@" + r.GetString(5)), r.GetDateTime(2), DateTime.Today.AddDays(-1));

              blpapi.sendHistDataRequestList(new List<string> { r.GetString(1) + (string.IsNullOrEmpty(r.GetString(5)) ? "" : "@" + r.GetString(5)) }, flist, "", r.GetString(4), r.GetDateTime(2), DateTime.Today.AddDays(-1), e =>
              {
                if (e.HasElement("securityError"))
                {
                  Element securityError = e.GetElement("securityError");
                  System.Console.WriteLine(string.Format("Ticker \"{0}\" - responseError", r.GetString(1)));
                  SqlCommand cmd = c.Command(sqlu)
                    .AddParameter("@SecurityID", r.GetInt32(0))
                    .AddParameter("@ISIN", r.GetString(1))
                    .AddParameter("@Date", DateTime.Today)
                    .AddParameter("@PCS", r.GetString(5));
                  foreach (var fe in flist)
                  {
                    cmd.AddParameter("@" + fe, "Err");
                  }
                  cmd.ExecuteNonQuery();
                }
                else
                {
                  System.Console.WriteLine(string.Format("Ticker \"{0}\"", e.GetElementAsString("security")));
                  Element fieldData = e.GetElement("fieldData");
                  for (int j = 0; j < fieldData.NumValues; j++)
                  {
                    Element element = fieldData.GetValueAsElement(j);
                    DateTime date = element.GetElementAsDatetime("date").ToSystemDateTime();
                    SqlCommand cmd = c.Command(sqlu)
                      .AddParameter("@SecurityID", r.GetInt32(0))
                      .AddParameter("@ISIN", r.GetString(1))
                      .AddParameter("@Date", date)
                      .AddParameter("@PCS", r.GetString(5));
                    foreach (var fe in flist)
                    {
                      cmd.AddParameter("@" + fe, (element.HasElement(fe) ? (object)element.GetElementAsString(fe) : DBNull.Value));
                    }
                    cmd.ExecuteNonQuery();
                    System.Console.WriteLine(string.Format("{0} {1:dd.MM.yyyy} {2} {3}", e.GetElementAsString("security"), date, element.HasElement("VOLUME") ? element.GetElementAsString("VOLUME") : "", element.HasElement("PX_LAST") ? element.GetElementAsString("PX_LAST") : "", element.HasElement("CRNCY_ADJ_PX_LAST") ? element.GetElementAsString("CRNCY_ADJ_PX_LAST") : ""));
                  }
                }
              });
            });
        });
      }
    }

    private void run9()
    {
      SqlHelper _sqlHelper = new SqlHelper();
      var list = new List<Security>();
      string sqlu = "insert pSecDiv (SecurityID, ISIN, DeclaredDate, ExDate, RecordDate, PayableDate, DivAmount, DivFrequency, DivType) values (@SecurityID, @ISIN, @DeclaredDate, @ExDate, @RecordDate, @PayableDate, @DivAmount, @DivFrequency, @DivType)";
      _sqlHelper.GetConnection().Using(c =>
      {
        c.Command("delete pSecDiv").ExecuteNonQuery();
        c.Command("up_avgGetBlmSecDiv")
          .ExecuteReader()
          .ReadAll(r =>
          {
            list.Add(new Security() { SecurityID = r.GetInt32(0), Ticker = r.GetString(1), Override = string.Format("DVD_START_DT={0:yyyyMMdd};DVD_END_DT={1:yyyyMMdd}", DateTime.Today.AddDays(-90), DateTime.Today.AddDays(1)) });
          });
        Blpapi blpapi = new Blpapi();
        if (blpapi.Connect())
        {
          foreach (Security s in list)
          {
            System.Console.WriteLine(string.Format("Tiker={0} ID={1}", s.Ticker, s.SecurityID));
            blpapi.sendRefDataBulkRequest(s.SecurityID, s.Ticker, "DVD_HIST", s.Override, e =>
            {
              c.Command(sqlu)
                .AddParameter("@SecurityID", s.SecurityID)
                .AddParameter("@ISIN", s.Ticker)
                .AddParameter("@DeclaredDate", !e.IsNullValue(0) ? (object)e.GetElement(0).GetValueAsDate().ToSystemDateTime() : DBNull.Value)
                .AddParameter("@ExDate", !e.IsNullValue(1) ? (object)e.GetElement(1).GetValueAsDate().ToSystemDateTime() : DBNull.Value)
                .AddParameter("@RecordDate", !e.IsNullValue(2) ? (object)e.GetElement(2).GetValueAsDate().ToSystemDateTime() : DBNull.Value)
                .AddParameter("@PayableDate", !e.IsNullValue(3) ? (object)e.GetElement(3).GetValueAsDate().ToSystemDateTime() : DBNull.Value)
                .AddParameter("@DivAmount", !e.IsNullValue(4) ? (object)e.GetElement(4).GetValueAsFloat64() : DBNull.Value)
                .AddParameter("@DivFrequency", !e.IsNullValue(5) ? (object)e.GetElement(5).GetValueAsString() : DBNull.Value)
                .AddParameter("@DivType", !e.IsNullValue(6) ? (object)e.GetElement(6).GetValueAsString() : DBNull.Value)
                .ExecuteNonQuery();
            });
          }
        }
      });
    }

    private void run10(DateTime dt)
    {
      SqlHelper _sqlHelper = new SqlHelper();
      var list = new List<Tuple<int, string>>();
      var slist = new List<string>();

      var sl = new List<Tuple<int, string, string, string>>();

      var flist = new List<string> { "PX_LAST", "HIGH", "LOW", "VOLUME", "PX_OPEN", "BID", "ASK" };
      var sb = new StringBuilder();
      sb.AppendLine("if object_id ('tempdb..pBloomRates10') is not null  drop table tempdb..pBloomRates10 create table tempdb..pBloomRates10 (SecurityID int, ISIN varchar(50), Date smalldatetime, PCS varchar(50)");
      var sbu1 = new StringBuilder();
      var sbu2 = new StringBuilder();
      foreach (var fe in flist)
      {
        sb.AppendFormat(",{0} varchar(50)", fe);
        sbu1.AppendFormat(",{0}", fe);
        sbu2.AppendFormat(",@{0}", fe);
      }
      sb.Append(", primary key (SecurityID, Date, PCS))");
      string sqlu = String.Format("insert tempdb..pBloomRates10(SecurityID,ISIN,Date,PCS{0}) values (@SecurityID,@ISIN,@Date,@PCS{1})", sbu1, sbu2);
      _sqlHelper.GetConnection().Using(c =>
      {
        c.Command(sb.ToString()).ExecuteNonQuery();
        c.Command("exec up_avgGetBlmRateSec3")
          .ExecuteReader()
          .ReadAll(r =>
          {
            //list.Add(r.GetInt32(0));
            //slist.Add(r.GetString(1));
            sl.Add(Tuple.Create(r.GetInt32(0), r.GetString(1), r.GetString(2), r.GetString(3)));
          });
        foreach (var val in sl.Select(p => p.Item3).Distinct())
        {
          list = sl.Where(p => p.Item3 == val).Select(p => Tuple.Create(p.Item1, p.Item4)).ToList();
          slist = sl.Where(p => p.Item3 == val).Select(p => p.Item2 + (string.IsNullOrEmpty(p.Item4) ? "" : "@" + p.Item4)).ToList();

          Blpapi blpapi = new Blpapi();
          if (blpapi.Connect())
          {
            //DateTime de = DateTime.Today.AddDays(DateTime.Today.DayOfWeek == DayOfWeek.Sunday ? -2 : DateTime.Today.DayOfWeek == DayOfWeek.Monday ? -3 : -1);
            //DateTime db = de.AddDays(de.DayOfWeek == DayOfWeek.Sunday ? -3 : de.DayOfWeek == DayOfWeek.Monday ? -4 : -2);
            DateTime de = dt;
            DateTime db = de.AddDays(de.DayOfWeek == DayOfWeek.Sunday ? -3 : de.DayOfWeek == DayOfWeek.Monday ? -4 : -2);

            blpapi.sendHistDataRequestList(slist, flist, "", val, de, de, e =>
            {
              string ticker = e.GetElementAsString("security");
              System.Console.WriteLine(string.Format("Ticker \"{0}\"", ticker));
              int rowIndex = e.GetElementAsInt32("sequenceNumber");
              if (e.HasElement("securityError"))
              {
                Element securityError = e.GetElement("securityError");
                System.Console.WriteLine(string.Format("Ticker \"{0}\" - responseError", ticker));
                SqlCommand cmd = c.Command(sqlu)
                  .AddParameter("@SecurityID", list[rowIndex].Item1)
                  .AddParameter("@ISIN", ticker)
                  .AddParameter("@Date", DateTime.Today)
                  .AddParameter("@PCS", list[rowIndex].Item2);
                foreach (var fe in flist)
                {
                  cmd.AddParameter("@" + fe, "Err");
                }
                cmd.ExecuteNonQuery();
              }
              else
              {
                Element fieldData = e.GetElement("fieldData");
                for (int j = 0; j < fieldData.NumValues; j++)
                {
                  Element element = fieldData.GetValueAsElement(j);
                  DateTime date = element.GetElementAsDatetime("date").ToSystemDateTime();
                  SqlCommand cmd = c.Command(sqlu)
                    .AddParameter("@SecurityID", list[rowIndex].Item1)
                    .AddParameter("@ISIN", ticker)
                    .AddParameter("@Date", date)
                    .AddParameter("@PCS", list[rowIndex].Item2);
                  foreach (var fe in flist)
                  {
                    cmd.AddParameter("@" + fe, (element.HasElement(fe) ? (object)element.GetElementAsString(fe) : DBNull.Value));
                  }
                  cmd.ExecuteNonQuery();
                  //System.Console.WriteLine(string.Format("{0} {1:dd.MM.yyyy} {2} {3}", ticker, date, element.HasElement("VOLUME") ? element.GetElementAsString("VOLUME") : "", element.HasElement("PX_LAST") ? element.GetElementAsString("PX_LAST") : ""));
                }
              }
            });
          }
        }
      });
    }

    private void run11()
    {
      SqlHelper _sqlHelper = new SqlHelper();
      _sqlHelper.GetConnection().Using(c =>
      {
        c.Command("exec msdb..sp_start_job @job_id='{1E95E152-E3E2-414A-942C-71147BC1CA1B}'").ExecuteNonQuery();
      });
    }

    private void run13()
    {
      SqlHelper _sqlHelper = new SqlHelper();
      _sqlHelper.GetConnection().Using(c =>
      {
        c.Command("exec up_avgParseRatingBlmberg").ExecuteNonQuery();
      });
    }

    private void run15()
    {
      SqlHelper _sqlHelper = new SqlHelper();
      _sqlHelper.GetConnection().Using(c =>
      {
        c.Command("exec msdb..sp_start_job @job_id='{45F588B4-9496-4B7C-8203-32E0F3CD1F83}'").ExecuteNonQuery();
      });
    }

    private void run16()
    {
      SqlHelper _sqlHelper = new SqlHelper();
      _sqlHelper.GetConnection().Using(c =>
      {
        c.Command("insert tExchangeIndexValue (ExchangeIndexID, Date, Open_value, Max_value, Min_value, Close_value, Caps) select ei.ExchangeIndexID, b.Date, 0, 0, 0, b.PX_LAST, 0 from tempdb..pBloomIndex b join tExchangeIndex ei on ei.Brief = b.IndexID where b.PX_Last <> 'Err' and not exists(select 1 from tExchangeIndexValue where ExchangeIndexID = ei.ExchangeIndexID and Date = b.Date)").ExecuteNonQuery();
        c.Command("update tExchangeIndexValue set Close_value = cast(bi.PX_LAST as money) from tempdb..pBloomIndex bi join tExchangeIndex ei on ei.Brief = bi.IndexID join tExchangeIndexValue eiv on eiv.ExchangeIndexID = ei.ExchangeIndexID and eiv.Date = bi.Date where PX_Last <> 'Err' and eiv.Close_value <> cast(bi.PX_LAST as money)").ExecuteNonQuery();
      });
    }

    private void run18()
    {
      SqlHelper _sqlHelper = new SqlHelper();
      _sqlHelper.GetConnection().Using(c =>
      {
        c.Command("exec msdb..sp_start_job @job_id='{DE4A3652-5396-4A8B-9BB3-4BD336489480}'").ExecuteNonQuery();
      });
    }

    private void run20()
    {
      SqlHelper _sqlHelper = new SqlHelper();
      _sqlHelper.GetConnection().Using(c =>
      {
        c.Command("exec msdb..sp_start_job @job_id='{9FAB1368-AEA9-4242-BE5A-C11514E91000}'").ExecuteNonQuery();
      });
    }

  }
}
