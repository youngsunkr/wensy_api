using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Services;

using System.Text;
using System.Data;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Data.SqlClient;


namespace SimpleWebService
{
    /// <summary>
    /// Summary description for AgentInitial
    /// </summary>
    [WebService(Namespace = "http://sqlmvp.kr")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // To allow this Web Service to be called from script, using ASP.NET AJAX, uncomment the following line. 
    // [System.Web.Script.Services.ScriptService]
    public class AgentInitial : System.Web.Services.WebService
    {

        string strSPConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings["SPConnectionString"].ConnectionString;

        [WebMethod]
        public int UpdateHoststatus(string CurrentStatus, int iServerNumber, string TimeIn, string TimeIn_UTC)
        {

            try
            {
                string strQuery_info = @"UPDATE tbHostStatus SET CurrentStatus = '" + CurrentStatus + "', TimeIn_UTC = '" + TimeIn_UTC + "', TimeIn = '" + TimeIn + "' where ServerNum = " + iServerNumber;
                RunQueryCommand(strQuery_info);
            }
            catch (Exception ex)
            {
                TestLogFiles("AgentInitial UpdateHostatus Error : " + ex.Message);
            }
            
            return iServerNumber;
        }

        [WebMethod]
        public int UpdateHoststatus_Version(string HostName, string CurrentStatus, string RAMSize, string WinVer, string Processors, string IPAddress, string AgentVer, int iServerNumber)
        {
            try
            {
                string strQuery_info = @"UPDATE tbHostStatus SET Hostname = '" + HostName + "', CurrentStatus = '" + CurrentStatus + "', RAMSize = '" + RAMSize + "', WinVer = '" + WinVer + "', Processors = '" + Processors + "', IPAddress = '" + IPAddress + "', AgentVer = '" + AgentVer + "' where ServerNum = " + iServerNumber.ToString();
                RunQueryCommand(strQuery_info);

            }
            catch (Exception ex)
            {
                TestLogFiles("AgentInitial UpdateHoststatusTable_Verion Error : " + ex.Message);
            }

            return iServerNumber;
        }

        //Agent 로그인시 Product Key 조회
        [WebMethod]
        public Byte[] AgentLogin(string ProductKey)
        {
            DataTable dtResult = new DataTable();
            
            //무조건 서버 타입이 SQL로 반환되도록 쿼리 수정
            string QueryString = "select ServerNum, ServerType, DisplayName, DisplayGroup, CompanyNum from tbHostStatus where ProductKey = '" + ProductKey + "'";
            dtResult = RunSelectAdhoc(QueryString);

            BinaryFormatter bformatter = new BinaryFormatter();
            MemoryStream stream = new MemoryStream();

            if (dtResult != null)
            {
                bformatter.Serialize(stream, dtResult);
                byte[] b = stream.ToArray();
                stream.Close();
                return b;
            }

            return null;
        }

        //Alert Rule List 조회
        [WebMethod]
        public Byte[] AlertRuleList(string ServerNum)
        {
            DataTable dtResult = new DataTable();

            string QueryString = "select ServerNum, ServerType, ReasonCode, PCID, Threshold, TOperator, ReasonCodeDesc, InstanceName, Duration, HasReference, RecordApps, IsEnabled, AlertLevel, RefDescription, ReqActionCode from tbAlertRules_Server where ServerNum = '" + ServerNum + "'";
            dtResult = RunSelectAdhoc(QueryString);

            BinaryFormatter bformatter = new BinaryFormatter();
            MemoryStream stream = new MemoryStream();

            if (dtResult != null)
            {
                bformatter.Serialize(stream, dtResult);
                byte[] b = stream.ToArray();
                stream.Close();
                return b;
            }

            return null;
        }

        //PCID List 조회
        [WebMethod]
        public Byte[] PCIDList(string ServerNum)
        {
            DataTable dtResult = new DataTable();

            string QueryString = "select ServerNum, PCID, ServerType, PObjectName, PCounterName, HasInstance, ValueDescription, RValueDescription, used from tbPCID_Server where ServerNum = '" + ServerNum + "'";
            dtResult = RunSelectAdhoc(QueryString);

            BinaryFormatter bformatter = new BinaryFormatter();
            MemoryStream stream = new MemoryStream();

            if (dtResult != null)
            {
                bformatter.Serialize(stream, dtResult);
                byte[] b = stream.ToArray();
                stream.Close();
                return b;
            }

            return null;
        }

        //PCID Instance List 조회
        [WebMethod]
        public Byte[] PCIDInstanceList(string ServerNum)
        {
            DataTable dtResult = new DataTable();

            string QueryString = "select ServerNum, PCID, InstanceName, IfContains from tbPInstance_Server where ServerNum = '" + ServerNum + "'";
            dtResult = RunSelectAdhoc(QueryString);

            BinaryFormatter bformatter = new BinaryFormatter();
            MemoryStream stream = new MemoryStream();

            if (dtResult != null)
            {
                bformatter.Serialize(stream, dtResult);
                byte[] b = stream.ToArray();
                stream.Close();
                return b;
            }

            return null;
        }

        //Query Define List List 조회
        [WebMethod]
        public Byte[] QueryDefineList(string ServerNum)
        {
            DataTable dtResult = new DataTable();

            string QueryString = "select QueryID, Interval, DestinationTable, Query, [Enabled], QueryDescription, SPName, OccursTime, IsProcedure from tbSQLQueryDefinition_Server where ServerNum = " + ServerNum;
            dtResult = RunSelectAdhoc(QueryString);

            BinaryFormatter bformatter = new BinaryFormatter();
            MemoryStream stream = new MemoryStream();

            if (dtResult != null)
            {
                bformatter.Serialize(stream, dtResult);
                byte[] b = stream.ToArray();
                stream.Close();
                return b;
            }

            return null;
        }
        
        void RunQueryCommand(string strQuery)
        {

            using (SqlConnection sqlConn = new SqlConnection(strSPConnectionString))
            {
                try
                {
                    SqlCommand sqlComm = new SqlCommand();
                    sqlComm = sqlConn.CreateCommand();

                    sqlComm.CommandText = strQuery;

                    sqlConn.Open();
                    sqlComm.ExecuteNonQuery();

                    sqlConn.Close();
                }
                catch (Exception ex)
                {
                    TestLogFiles("AgentInitial RunQueryCommand Error - " + ex.Message + " : " + strQuery);
                }
            }
        }

        
        private DataTable RunSelectAdhoc(string QueryString)
        {

            DataTable dtResult = new DataTable();

            using (SqlConnection sqlconn = new SqlConnection(strSPConnectionString))
            {
                try
                {
                    SqlDataAdapter da = new SqlDataAdapter();
                    da.SelectCommand = new SqlCommand(QueryString, sqlconn);
                    da.SelectCommand.CommandType = CommandType.Text;

                    da.Fill(dtResult);

                    return dtResult;
                }
                catch (Exception ex)
                {
                    TestLogFiles("AgentInitial - Select Ad-hoc Failed. - " + QueryString + "," + ex.Message);
                    return null;
                }
                finally
                {
                    sqlconn.Close();
                }
            }
        }

        void TestLogFiles(string strLogMsg)
        {
            string strPath = System.Configuration.ConfigurationManager.AppSettings["LogPath"].ToString();

            try
            {
                using (StreamWriter sw = new StreamWriter(strPath, true))
                {
                    sw.WriteLine(DateTime.Now.ToString() + ", " + strLogMsg);
                    sw.Close();
                }
            }
            catch (Exception ex)
            {
                return;
            }
        }

        private DataTable BytesToDataTable(Byte[] byteArray)
        {
            DataTable dtResult = new DataTable();

            BinaryFormatter bformatter = new BinaryFormatter();
            MemoryStream stream = new MemoryStream();

            try
            {
                if (byteArray != null)
                {
                    bformatter = new BinaryFormatter();
                    stream = new MemoryStream(byteArray);
                    dtResult = (DataTable)bformatter.Deserialize(stream);
                }
                else
                    dtResult = null;
            }
            catch (Exception ex)
            {
                return null;
            }

            return dtResult;
        }

        private Byte[] DataTableToBytes(DataTable dtInput)
        {
            BinaryFormatter bformatter = new BinaryFormatter();
            MemoryStream stream = new MemoryStream();

            try
            {
                if (dtInput != null)
                {
                    bformatter.Serialize(stream, dtInput);
                    byte[] b = stream.ToArray();
                    stream.Close();
                    return b;
                }
                return null;
            }
            catch (Exception ex)
            {
                return null;
            }

        }

        private static byte[] StrToByteArray(string str)
        {
            UTF8Encoding encoding = new UTF8Encoding();
            return encoding.GetBytes(str);
        }

        private static string ByteArrayToStr(byte[] barr)
        {
            UTF8Encoding encoding = new UTF8Encoding();
            return encoding.GetString(barr, 0, barr.Length);
        }







    }
}
