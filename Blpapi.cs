using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Bloomberglp.Blpapi.BMpipeApp
{
  public class Blpapi
  {
    private Session d_session;

    public Blpapi()
    {
    }

    public bool Connect()
    {
      SessionOptions sessionOptions = new SessionOptions() { ServerHost = "localhost", ServerPort = 8194 };
      if (d_session != null) d_session.Stop();
      d_session = new Session(sessionOptions);
      if (!d_session.Start())
      {
        System.Console.WriteLine("Could not start session.");
        return false;
      }
      if (!d_session.OpenService("//blp/refdata"))
      {
        System.Console.WriteLine("Could not open service //blp/refdata");
        return false;
      }
      return true;
    }

    public void sendRefDataBulkRequest(int securityID, string security, string fields, string overrides, Action<Element> action)
    {
      CorrelationID requestID = new CorrelationID(1);
      Service refDataSvc = d_session.GetService("//blp/refdata");
      Request request = refDataSvc.CreateRequest("ReferenceDataRequest");
      request.Append("securities", security);
      request.Append("fields", fields);

      if (overrides != "")
      {
        foreach (var o in overrides.Split(new char[] { ';' }))
        {
          string[] _overrides = o.Split(new char[] { '=' });
          if (_overrides.Count() == 2)
          {
            Element override1 = request["overrides"].AppendElement();
            override1.SetElement("fieldId", _overrides[0]);
            override1.SetElement("value", _overrides[1]);
          }
        }
      }

      //Element overrides = request["overrides"];
      //Element override1 = overrides.AppendElement();
      //string[] _overrides = _override.Split(new char[] { '=' });
      //if (_overrides.Count() == 2)
      //{
      //  override1.SetElement("fieldId", _overrides[0]);
      //  override1.SetElement("value", _overrides[1]);
      //}
      d_session.SendRequest(request, requestID);
      bool continueToLoop = true;
      while (continueToLoop)
      {
        Event eventObj = d_session.NextEvent();
        switch (eventObj.Type)
        {
          case Event.EventType.RESPONSE:
            continueToLoop = false;
            handleResponseEventBulk(securityID, eventObj, action);
            break;
          case Event.EventType.PARTIAL_RESPONSE:
            handleResponseEventBulk(securityID, eventObj, action);
            break;
          default:
            handleOtherEvent(eventObj);
            break;
        }
      }
    }

    private void handleResponseEventBulk(int securityID, Event eventObj, Action<Element> action)
    {
      System.Console.WriteLine("EventType =" + eventObj.Type);
      foreach (Message message in eventObj.GetMessages())
      {
        if (message.HasElement("responseError"))
        {
          //printErrorInfo("REQUEST FAILED: ", msg.GetElement(RESPONSE_ERROR));
          continue;
        }
        if (message.MessageType.Equals(Bloomberglp.Blpapi.Name.GetName("ReferenceDataResponse")))
        {
          Element securities = message.GetElement("securityData");
          for (int secCnt = 0; secCnt < securities.NumValues; ++secCnt)
          {
            Element security = securities.GetValueAsElement(secCnt);
            string ticker = security.GetElementAsString("security");
            if (security.HasElement("securityError"))
            {
              Element securityError = security.GetElement("securityError");
              return;
            }
            else
            {
              Element fields = security.GetElement("fieldData");
              for (int eleCtr = 0; eleCtr < fields.NumElements; ++eleCtr)
              {
                Element field = fields.GetElement(eleCtr);
                if (field.Datatype == Bloomberglp.Blpapi.Schema.Datatype.SEQUENCE)
                {
                  for (int bvCtr = 0; bvCtr < field.NumValues; bvCtr++)
                  {
                    Element bulkElement = field.GetValueAsElement(bvCtr);
                    action(bulkElement);
                  }
                }
                else
                {

                }
              }
            }
          }

          //System.Console.WriteLine("correlationID=" + message.CorrelationID);
          //System.Console.WriteLine("messageType =" + message.MessageType);
          //message.Print(System.Console.Out);
        }
      }
    }

    private void handleOtherEvent(Event eventObj)
    {
      System.Console.WriteLine("EventType=" + eventObj.Type);
      foreach (Message message in eventObj.GetMessages())
      {
        System.Console.WriteLine("correlationID=" + message.CorrelationID);
        System.Console.WriteLine("messageType=" + message.MessageType);
        //message.Print(System.Console.Out);
        if (Event.EventType.SESSION_STATUS == eventObj.Type && message.MessageType.Equals("SessionTerminated"))
        {
          System.Console.WriteLine("Terminating: " + message.MessageType);
          System.Environment.Exit(1);
        }
      }
    }

    public void sendRefDataRequest(string security, string fields, string _override, Action<Element> action)
    {
      CorrelationID requestID = new CorrelationID(1);
      Service refDataSvc = d_session.GetService("//blp/refdata");
      Request request = refDataSvc.CreateRequest("ReferenceDataRequest");
      request.Append("securities", security);
      request.Append("fields", fields);
      Element overrides = request["overrides"];
      string[] _overrides = _override.Split(new char[] { '=' });
      if (_overrides.Count() == 2)
      {
        Element override1 = overrides.AppendElement();
        override1.SetElement("fieldId", _overrides[0]);
        override1.SetElement("value", _overrides[1]);
      }
      d_session.SendRequest(request, requestID);
      bool continueToLoop = true;
      while (continueToLoop)
      {
        Event eventObj = d_session.NextEvent();
        switch (eventObj.Type)
        {
          case Event.EventType.RESPONSE:
            continueToLoop = false;
            handleResponseEvent(eventObj, action);
            break;
          case Event.EventType.PARTIAL_RESPONSE:
            handleResponseEvent(eventObj, action);
            break;
          default:
            handleOtherEvent(eventObj);
            break;
        }
      }
    }

    private void handleResponseEvent(Event eventObj, Action<Element> action)
    {
      foreach (Message message in eventObj.GetMessages())
      {
        //message.Print(System.Console.Out);
        if (message.HasElement("responseError"))
        {
          System.Console.WriteLine("responseError");
          continue;
        }
        if (message.MessageType.Equals(Bloomberglp.Blpapi.Name.GetName("ReferenceDataResponse")))
        {
          Element securities = message.GetElement("securityData");
          for (int i = 0; i < securities.NumValues; ++i)
          {
            Element security = securities.GetValueAsElement(i);
            action(security);
          }

          //System.Console.WriteLine("correlationID=" + message.CorrelationID);
          //System.Console.WriteLine("messageType =" + message.MessageType);
          //message.Print(System.Console.Out);
        }
        else if (message.MessageType.Equals(Bloomberglp.Blpapi.Name.GetName("HistoricalDataResponse")))
        {
          //message.Print(System.Console.Out);
          Element security = message.GetElement("securityData");
          action(security);
        }
      }
    }

    public void sendRefDataRequestList(List<string> securities, List<string> fields, string overrides, Action<Element> action)
    {
      CorrelationID requestID = new CorrelationID(1);
      Service refDataSvc = d_session.GetService("//blp/refdata");
      Request request = refDataSvc.CreateRequest("ReferenceDataRequest");
      foreach (string security in securities)
        request.Append("securities", security);
      foreach (string f in fields)
        request.Append("fields", f);

      if (overrides != "")
      {
        foreach (var o in overrides.Split(new char[] { ';' }))
        {
          string[] _overrides = o.Split(new char[] { '=' });
          if (_overrides.Count() == 2)
          {
            Element override1 = request["overrides"].AppendElement();
            override1.SetElement("fieldId", _overrides[0]);
            override1.SetElement("value", _overrides[1]);
          }
        }
      }
      d_session.SendRequest(request, requestID);
      bool continueToLoop = true;
      while (continueToLoop)
      {
        Event eventObj = d_session.NextEvent();
        switch (eventObj.Type)
        {
          case Event.EventType.RESPONSE:
            continueToLoop = false;
            handleResponseEvent(eventObj, action);
            break;
          case Event.EventType.PARTIAL_RESPONSE:
            handleResponseEvent(eventObj, action);
            break;
          default:
            handleOtherEvent(eventObj);
            break;
        }
      }
    }

    public void sendHistDataRequestList(List<string> securities, List<string> fields, string overrides, string currency, DateTime ds, DateTime de, Action<Element> action)
    {
      CorrelationID requestID = new CorrelationID(1);
      Service refDataSvc = d_session.GetService("//blp/refdata");
      Request request = refDataSvc.CreateRequest("HistoricalDataRequest");
      foreach (string security in securities)
        request.Append("securities", security);
      foreach (string f in fields)
        request.Append("fields", f);
      if (overrides != "")
      {
        foreach (var o in overrides.Split(new char[] { ';' }))
        {
          string[] _overrides = o.Split(new char[] { '=' });
          if (_overrides.Count() == 2)
          {
            Element override1 = request["overrides"].AppendElement();
            override1.SetElement("fieldId", _overrides[0]);
            override1.SetElement("value", _overrides[1]);
            Console.Write(_overrides[0]);
            Console.Write("=");
            Console.WriteLine(_overrides[1]);
          }
        }
      }
      if (currency != "")
        request.Set("currency", currency);
      request.Set("adjustmentSplit", false);
      request.Set("startDate", string.Format("{0:yyyyMMdd}", ds));
      request.Set("endDate", string.Format("{0:yyyyMMdd}", de));

      d_session.SendRequest(request, requestID);
      bool continueToLoop = true;
      while (continueToLoop)
      {
        Event eventObj = d_session.NextEvent();
        switch (eventObj.Type)
        {
          case Event.EventType.RESPONSE:
            continueToLoop = false;
            handleResponseEvent(eventObj, action);
            break;
          case Event.EventType.PARTIAL_RESPONSE:
            handleResponseEvent(eventObj, action);
            break;
          default:
            handleOtherEvent(eventObj);
            break;
        }
      }
    }

  }
}
