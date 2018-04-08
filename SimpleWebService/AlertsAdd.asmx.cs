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
using Newtonsoft.Json;


namespace SimpleWebService
{
    /// <summary>
    /// AlertsAdd의 요약 설명입니다.
    /// </summary>
    [WebService(Namespace = "http://sqlmvp.kr")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // ASP.NET AJAX를 사용하여 스크립트에서 이 웹 서비스를 호출하려면 다음 줄의 주석 처리를 제거합니다. 
    // [System.Web.Script.Services.ScriptService]
    public class AlertsAdd : System.Web.Services.WebService
    {
        string strSPConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings["SPConnectionString"].ConnectionString;

        [WebMethod]
        public int AddAlerts(Byte[] bytearr, int iServerNumber, string strTimeIn, string strTimeIn_UTC)
        {
            DataTable dtAlerts = new DataTable();

            if (bytearr == null)
                return 0;

            dtAlerts = BytesToDataTable(bytearr);

            if (dtAlerts == null)
                return 0;

            string jsonString = JsonConvert.SerializeObject(dtAlerts);
            //System.Diagnostics.Debug.WriteLine(jsonString);

            try
            {
                string strQuery = @"insert into tbAlerts_JSON (TimeIn_UTC, ServerNum, Data_JSON) values (N'" + strTimeIn_UTC + "', N'" + iServerNumber.ToString() + "', N'" + jsonString + "')";
                RunQueryCommand(strQuery);
            }
            catch (Exception ex)
            {
                TestLogFiles("Perf Insert Failed - tbAlerts_JSON Error : " + ex.Message);
            }

            //try
            //{
            //    string strRecord = "";
            //    string strRecord_info = "";

            //    foreach (DataRow dr in dtAlerts.Rows)
            //    {
            //        //if (dr["AlertStatus"].ToString() == "1")
            //        //{
            //        //    strRecord_info = " ('" + strTimeIn_UTC + "', '" + strTimeIn + "', " + iServerNumber.ToString() + ", N'" + dr["ReasonCode"].ToString() + "', N'" + dr["InstanceName"].ToString() + "', " + dr["PValue"].ToString() + ", N'" + dr["AlertStatus"].ToString() + "', N'" + dr["AlertDescription"].ToString() + "'),";
            //        //    strRecord_info = strRecord_info.Substring(0, strRecord_info.Length - 1);
            //        //    string strQuery_info = @"insert into tbAlerts_info (TimeIn_UTC, TimeIn, ServerNum, ReasonCode, InstanceName, PValue, AlertStatus, AlertDescription) values " + strRecord_info;

            //        //    RunQueryCommand(strQuery_info);
            //        //}
            //        //else
            //        {
            //            strRecord = " ('" + strTimeIn_UTC + "', '" + strTimeIn + "', " + iServerNumber.ToString() + ", N'" + dr["ReasonCode"].ToString() + "', N'" + dr["InstanceName"].ToString() + "', " + dr["PValue"].ToString() + ", N'" + dr["AlertStatus"].ToString() + "', N'" + dr["AlertDescription"].ToString() + "'),";
            //            strRecord = strRecord.Substring(0, strRecord.Length - 1);
            //            string strQuery = @"insert into tbAlerts (TimeIn_UTC, TimeIn, ServerNum, ReasonCode, InstanceName, PValue, AlertStatus, AlertDescription) values " + strRecord;

            //            RunQueryCommand(strQuery);
            //        }
            //    }
            //}
            //catch (Exception ex)
            //{
            //    TestLogFiles("Alerts Insert Failed - InsertPerfdataUsingAdHoc Error : " + ex.Message);
            //}

            return dtAlerts.Rows.Count;
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
                    TestLogFiles("Alerts Insert RunQueryCommand Error - " + ex.Message + " : " + strQuery);
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
