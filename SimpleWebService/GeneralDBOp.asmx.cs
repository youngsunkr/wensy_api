using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Services;

using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Web.Caching;

namespace SimpleWebService
{
    /// <summary>
    ///
    /// </summary>

    [WebService(Namespace = "http://sqlmvp.kr")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]

    // ASP.NET AJAX를 사용하여 스크립트에서 이 웹 서비스를 호출하려면 다음 줄의 주석 처리를 제거합니다. 
    // [System.Web.Script.Services.ScriptService]

    public class GeneralDBOp : System.Web.Services.WebService
    {
        string strSPConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings["SPConnectionString"].ConnectionString;
        int iServerNumber = 0;

        //const int MAX_PARAMETERS = 80;

        [WebMethod]
        public Byte[] SelectSPVaules(string strSPName, Byte[] btParams)
        {
            DataTable dtParms = new DataTable();
            DataTable dtResult = new DataTable();

            dtParms = BytesToDataTable(btParams);

            dtResult = RunSelectSP(dtParms, strSPName);

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

        [WebMethod]
        public int InsertSPValues(string strSPName, Byte[] btParams, Byte[] btValues)
        {
            DataTable dtValues = new DataTable();
            DataTable dtParms = new DataTable();

            dtParms = BytesToDataTable(btParams);
            dtValues = BytesToDataTable(btValues);

            iServerNumber = GetServerNumber(dtParms);
        
            return RunInsertSP(strSPName, dtParms, dtValues);
        }

        [WebMethod]
        public int UpdateSPValues(string strSPName, Byte[] btParams)
        {            
            DataTable dtValues = new DataTable();
            DataTable dtParms = new DataTable();

            dtParms = BytesToDataTable(btParams);

            iServerNumber = GetServerNumber(dtParms);       
           
            return UpdateDataUsingSP(dtParms, strSPName);
        }
        
        [WebMethod]
        public Byte[] TempTextOp(string strText, int iServerNumber)
        {
            if (strText.ToUpper().Contains("UPDATE") || strText.ToUpper().Contains("DELETE") || strText.ToUpper().Contains("DROP"))
                return null;

            DataTable dtResult = new DataTable();


            SqlConnection conReadQuery = new SqlConnection();
            conReadQuery = new SqlConnection(strSPConnectionString);
            string sCommand = strText;

            try
            {
                SqlDataAdapter da = new SqlDataAdapter(sCommand, conReadQuery);
                conReadQuery.Open();

                da.Fill(dtResult);

                if (dtResult != null)
                    return DataTableToBytes(dtResult);

                return null;
            }
            catch (Exception ex)
            {                
                return null;
            }
            finally
            {                
                conReadQuery.Close();
            }
            
        }

        [WebMethod]
        public int CopyTable(int iServerNumber, string strDest, Byte[] btValues)
        {
            DataTable dtValues = new DataTable();

            dtValues = BytesToDataTable(btValues);
            
            DataRow[] rowArray;

            using (SqlConnection con = new SqlConnection(strSPConnectionString))
            {
                try
                {
                    con.Open();

                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(con))
                    {
                        if (dtValues.Rows.Count > 0)
                        {
                            rowArray = dtValues.Select();
                            bulkCopy.DestinationTableName = "dbo." + strDest;
                            bulkCopy.WriteToServer(rowArray);
                        }
                    }
                }
                catch (Exception ex)
                {
                    TestLogFiles("Monitoring Query Insert failed. " + "Table Name : " + strDest + ", " + ex.Message);
                    return 0;
                }
                finally
                {
                    con.Close();
                }
            }

            return dtValues.Rows.Count;
        }


        [WebMethod]
        public int SQLMonitoringDataInsert(int iServerNumber, string strDest, Byte[] btValues, string strTimeIn, string strColsToInsert)
        {
            DataTable dtValues = new DataTable();

            dtValues = BytesToDataTable(btValues);

            //v3. Bulk Insert > Ad-Hoc Insert로 변경. 아래 Bulk Insert문 기능은 아래에 남겨두고, 사용안함.
            AdHocInsertData(iServerNumber, strDest, dtValues, strTimeIn, strColsToInsert);
            return dtValues.Rows.Count;

        }


        //IIS LOG INSERT
        [WebMethod]
        public int IISLogInsert(Byte[] btValues, int iServerNumber, string strTimeIn, string strTimeIn_UTC)
        {
            DataTable dtValues = new DataTable();

            dtValues = BytesToDataTable(btValues);

            InsertIISLogUsingAdHoc(dtValues, iServerNumber, strTimeIn, strTimeIn_UTC);
            return dtValues.Rows.Count;
        }

        //ServiceStatus INSERT
        [WebMethod]
        public int ServiceStatusInsert(Byte[] btValues, int iServerNumber, string strTimeIn, string strTimeIn_UTC)
        {
            DataTable dtValues = new DataTable();

            dtValues = BytesToDataTable(btValues);

            InsertServiceStatusUsingAdHoc(dtValues, iServerNumber, strTimeIn, strTimeIn_UTC);
            return dtValues.Rows.Count;
        }

        //Request Status INSERT
        [WebMethod]
        public int RequestStatusInsert(Byte[] btValues, int iServerNumber, string strTimeIn, string strTimeIn_UTC)
        {
            DataTable dtValues = new DataTable();

            dtValues = BytesToDataTable(btValues);

            InsertRequestStatusUsingAdHoc(dtValues, iServerNumber, strTimeIn, strTimeIn_UTC);
            return dtValues.Rows.Count;
        }

        //App Trace INSERT
        [WebMethod]
        public int ApptraceInsert(Byte[] btValues, int iServerNumber, string strTimeIn, string strTimeIn_UTC)
        {
            DataTable dtValues = new DataTable();

            dtValues = BytesToDataTable(btValues);

            InsertApptraceUsingAdHoc(dtValues, iServerNumber, strTimeIn, strTimeIn_UTC);
            return dtValues.Rows.Count;
        }


        
                 

        private void AdHocInsertData(int iServerNumber, string strDest, DataTable dtDataReceived, string strTimeIn, string strColsToInsert)
        {
            string strQuery = "";

            //          INSERT INTO table_name
            //              VALUES (value1,value2,value3,...)
            //             ('2015-10-08 06:53:22.887', '2015-10-08 06:53:07.000', 2152, 'P158', 4096, '0', 'C:'), 
            //             ('2015-10-08 06:53:22.887', '2015-10-08 06:53:07.000', 2152, 'P158', 4096, '0', 'C:')         

            string strRecord = "";
            string strDBTime = GetDBTime();
            int iCols = 0;

            iCols = dtDataReceived.Columns.Count;
            try
            { 
                foreach (DataRow dr in dtDataReceived.Rows)
                {

                    strRecord = " ('" + strDBTime + "', '" + strTimeIn + "', " + iServerNumber.ToString();

                    for (int i = 0; i < iCols; i++)
                    {

                        if (dr[i].GetType() == typeof(Int32) || dr[i].GetType() == typeof(Int64) || dr[i].GetType() == typeof(float) || dr[i].GetType() == typeof(double))
                            strRecord = strRecord + "," + dr[i].ToString() + " ";
                        else if (dr[i].GetType() == typeof(bool))
                        {
                            if (dr[i].ToString().ToUpper() == "TRUE")
                                strRecord = strRecord + ", 1 ";
                            else
                                strRecord = strRecord + ", 0 ";
                        }
                        else
                            strRecord = strRecord + ", '" + dr[i].ToString().Replace("'", "`") + "' ";
                    }

                    strRecord = strRecord + "),";

                    strRecord = strRecord.Substring(0, strRecord.Length - 1);
                    strQuery = @"insert into " + strDest + " (" + strColsToInsert + ")  VALUES  " + strRecord;

                    RunQueryCommand(strQuery);
                    //TestLogFiles("AdHocInsertData  : " + strQuery );
                }
                   
            }
            catch (Exception ex)
            {
                TestLogFiles("AdHocInsertData Failed - Error : " + strQuery + " : " + ex.Message);
            }
        }

       

        string GetDBTime()
        {

            using (SqlConnection sqlconn = new SqlConnection(strSPConnectionString))
            {
                try
                {
                    DataTable dtResult = new DataTable();
                    SqlDataAdapter da = new SqlDataAdapter();

                    da.SelectCommand = new SqlCommand("select getdate() as RegDate", sqlconn);
                    da.SelectCommand.CommandType = CommandType.Text;

                    da.Fill(dtResult);

                    if (dtResult != null)
                    {
                        if (dtResult.Rows.Count > 0)
                        {
                            foreach (DataRow dr in dtResult.Rows)
                            {
                                return Convert.ToDateTime(dr["RegDate"]).ToString("yyyy-MM-dd HH:mm:ss");
                            }
                        }
                    }
                    else
                    {
                        TestLogFiles("Perf Insert GetDBTime Error. p_DBTime return is null.");
                    }

                    return DateTime.Now.ToString();
                }
                catch (Exception ex)
                {
                    TestLogFiles("Perf Insert GetDBTime Error - " + ex.Message);
                    return DateTime.Now.ToString();
                }
            }

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
                    TestLogFiles("Insert AdHoc RunQueryCommand Error - " + ex.Message + " : " + strQuery);
                }
            }
        }


        [WebMethod]
        public int InsertQueryResultUsingSP(int iServerNumber, string strSPName, Byte[] btValues, string strTimeIn)
        {
            DataTable dtValues = new DataTable();
            DataTable dtResult = new DataTable();

            dtValues = BytesToDataTable(btValues);

            if (dtValues == null)
                return 0;

            try
            {
                using (SqlConnection sqlconn = new SqlConnection(strSPConnectionString))
                {
                    string strCommand = "";

                    try
                    {
                        sqlconn.Open();                        
                        string strRegDate = GetDBTime();

                        foreach (DataRow dr in dtValues.Rows)
                        {
                            strCommand = BuildSQLCommandString(strSPName, dr, iServerNumber, strTimeIn, strRegDate);
                            SqlCommand command = new SqlCommand(strCommand, sqlconn);

                            int iRows = command.ExecuteNonQuery();
                        }

                        return 1;
                    }
                    catch (Exception ex)
                    {
                        TestLogFiles("Single SP failed." + strSPName + ", Server Number = " + iServerNumber.ToString() + "," + ex.Message + " : " + strCommand);
                        return 0;
                    }
                    finally
                    {
                        sqlconn.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                TestLogFiles("InsertQueryResultUsingSP failed." + strSPName + "," + ex.Message);
                return 0;
            }

        }

        string BuildSQLCommandString(string strSPName, DataRow dr, int iServerNumber, string strTimeIn, string strRegDate)
        {
            // SP를 파라미터와 함께 실행하는 명령어 형태로 만든다. 예) exec [m_tbAlertFilters_List] '1', 'Windows', 'Warning'
            string strCommand = "";
            
            strCommand = "exec [" + strSPName + "] '" + strRegDate + "', '" + strTimeIn + "', '" + iServerNumber.ToString() + "', " ;

            for (int i = 0; i < dr.Table.Columns.Count; i++)
            {
                strCommand += " '" + dr[i].ToString() + "',";                

                //if(dr.Table.Columns[i].ColumnName == "TimeIn")       // 처음 2개 (RegDate, TimeIn)은 별도 처리.
                //    strCommand += " '" + Convert.ToDateTime(dr[i].ToString()).ToString("yyyy-MM-dd HH:mm:ss") + "',";
                //else
                //    strCommand += " '" + dr[i].ToString() + "',";                
            }

            strCommand = strCommand.Substring(0, strCommand.Length - 1);
            //마지막 쉽표 제거.

            // test code
            //TestLogFiles("BuildSQLCommandString : " + strCommand);

            return strCommand;
        }

       

        void InsertIISLogUsingAdHoc(DataTable dtPValues, int iServerNumber, string strTimeIn, string strTimeIn_UTC)
        {
            try
            {
                string strRecord = "";
                string strDBTime = GetDBTime();

                foreach (DataRow dr in dtPValues.Rows)
                {
                    strRecord += " ('" + strTimeIn_UTC + "', '" + strTimeIn + "', " + iServerNumber.ToString() + ", N'" + dr["SiteName"].ToString() + "', N'" + dr["HostHeader"].ToString() + "', N'" + dr["URI"].ToString() + "', " + dr["Hits"].ToString() + ", " + dr["MaxTimeTaken"].ToString() + ", " + dr["AvgTimeTaken"].ToString() + ", " + dr["TotalSCBytes"].ToString() + ", " + dr["TotalCSBytes"].ToString() + ", " + dr["StatusCode"].ToString() + ", " + dr["Win32StatusCode"].ToString() + "),";
                }

                strRecord = strRecord.Substring(0, strRecord.Length - 1);

                string strQuery = @"insert into tbIISLog (TimeIn_UTC, TimeIn, ServerNum, SiteName, HostHeader, URI,Hits, MaxTimeTaken, AvgTimeTaken, SCBytes, CSBytes, StatusCode, Win32StatusCode) VALUES ";
                strQuery += strRecord;

                RunQueryCommand(strQuery);
            }
            catch (Exception ex)
            {
                TestLogFiles("IIS Log Insert Failed - InsertPerfdataUsingAdHoc Error : " + ex.Message);
            }

        }

        void InsertServiceStatusUsingAdHoc(DataTable dtPValues, int iServerNumber, string strTimeIn, string strTimeIn_UTC)
        {
            try
            {
                string strRecord = "";
                string strDBTime = GetDBTime();

                foreach (DataRow dr in dtPValues.Rows)
                {
                    strRecord += " ('" + strTimeIn_UTC + "', '" + strTimeIn + "', " + iServerNumber.ToString() + ", N'" + dr["HostHeader"].ToString() + "', N'" + dr["SiteName"].ToString() + "', " + dr["TotalHits"].ToString() + ", " + dr["TotalSCBytes"].ToString() + ", " + dr["TotalCSBytes"].ToString() + ", " + dr["TotalCIP"].ToString() + ", " + dr["TotalErrors"].ToString() + ", '" + dr["AnalyzedLogTime"].ToString() + "'),";
                }

                strRecord = strRecord.Substring(0, strRecord.Length - 1);

                string strQuery = @"insert into tbIISServiceStatus (TimeIn_UTC,TimeIn,ServerNum,HostHeader,SiteName,TotalHits,TotalSCBytes,TotalCSBytes,TotalCIP,TotalErrors,AnalyzedLogTime) VALUES ";
                strQuery += strRecord;

                RunQueryCommand(strQuery);
            }
            catch (Exception ex)
            {
                TestLogFiles("ServiceStatus Insert Failed - InsertPerfdataUsingAdHoc Error : " + ex.Message);
            }

        }

        void InsertRequestStatusUsingAdHoc(DataTable dtPValues, int iServerNumber, string strTimeIn, string strTimeIn_UTC)
        {
            try
            {
                string strRecord = "";
                string strDBTime = GetDBTime();

                foreach (DataRow dr in dtPValues.Rows)
                {
                    strRecord += " ('" + strTimeIn_UTC + "', '" + strTimeIn + "', " + iServerNumber.ToString() + ", N'" + dr["HostHeader"].ToString() + "', N'" + dr["SiteName"].ToString() + "', N'" + dr["ValueDescription"].ToString() + "', " + dr["TotalNumber"].ToString() + ", N'" + dr["LogValue"].ToString() + "'),";
                }

                strRecord = strRecord.Substring(0, strRecord.Length - 1);

                string strQuery = @"insert into tbIISRequestStatus (TimeIn_UTC, TimeIn, ServerNum, HostHeader, SiteName, ValueDescription, TotalNumber, LogValue) VALUES ";
                strQuery += strRecord;

                RunQueryCommand(strQuery);
            }
            catch (Exception ex)
            {
                TestLogFiles("Request Status Insert Failed - InsertPerfdataUsingAdHoc Error : " + ex.Message);
            }

        }

        void InsertApptraceUsingAdHoc(DataTable dtPValues, int iServerNumber, string strTimeIn, string strTimeIn_UTC)
        {
            try
            {
                string strRecord = "";
                string strDBTime = GetDBTime();

                foreach (DataRow dr in dtPValues.Rows)
                {
                    strRecord += " ('" + strTimeIn_UTC + "', '" + strTimeIn + "', " + iServerNumber.ToString() + ", N'" + dr["ReasonCode"].ToString() + "', N'" + dr["URI"].ToString() + "', N'" + dr["ClientLocation"].ToString() + "', " + dr["RunningTime"].ToString() + "),";
                }

                strRecord = strRecord.Substring(0, strRecord.Length - 1);

                string strQuery = @"insert into tbIISAppTrace (TimeIn_UTC, TimeIn, ServerNum, ReasonCode, URI, ClientLocation, RunningTime) VALUES ";
                strQuery += strRecord;

                RunQueryCommand(strQuery);
            }
            catch (Exception ex)
            {
                TestLogFiles("Apptrace Insert Failed - InsertPerfdataUsingAdHoc Error : " + ex.Message);
            }

        }

       

        private int RunInsertSP(string strSPName,DataTable dtParms, DataTable dtValues)
        {
            bool bIsOK = false;

            try
            {
                if (dtValues != null)
                {
                    foreach (DataRow dr in dtValues.Rows)
                    {
                        bIsOK = InsertDataUsingSP(dr, dtParms, strSPName);
                        if (!bIsOK)
                            return 0;
                    }
                }
                return dtValues.Rows.Count;
            }
            catch (Exception ex)
            {                
                return 0;
            }
        }

        private int UpdateDataUsingSP(DataTable dtParms, string strSPName)
        {
            //DataTable dtResult = new DataTable();
            
            using (SqlConnection sqlconn = new SqlConnection(strSPConnectionString))
            {
                try
                {
                    sqlconn.Open();
                    SqlDataAdapter da = new SqlDataAdapter();
                    da.UpdateCommand = new SqlCommand(strSPName, sqlconn);
                    da.UpdateCommand.CommandType = CommandType.StoredProcedure;

                    AddSingleParameters(da.UpdateCommand, dtParms);

                    //da.Fill(dtResult);
                    da.UpdateCommand.ExecuteNonQuery();
                    da.UpdateCommand.Parameters.Clear();

                    return 1;
                }
                catch (Exception ex)
                {                    
                    return 0;
                }
                finally
                {
                    sqlconn.Close();
                }
            }

        }

        private bool InsertDataUsingSP(DataRow dr, DataTable dtParms, string strSPName)
        {

            using (SqlConnection sqlconn = new SqlConnection(strSPConnectionString))
            {
                try
                {
                    sqlconn.Open();
                    SqlDataAdapter da = new SqlDataAdapter();
                    da.InsertCommand = new SqlCommand(strSPName, sqlconn);
                    da.InsertCommand.CommandType = CommandType.StoredProcedure;

                    AddInsertParameters(da.InsertCommand, dtParms, dr);
                    
                    da.InsertCommand.ExecuteNonQuery();
                    da.InsertCommand.Parameters.Clear();

                    return true;
                }
                catch (Exception ex)
                {
                    TestLogFiles("General DB Op - Insert SP failed. " + strSPName + ", " + ex.Message);
                    return false;
                }
                finally
                {
                    sqlconn.Close();
                }
            }

        }

        private void AddInsertParameters(SqlCommand sqlcmd, DataTable dtParms, DataRow drValue)
        {

            if (dtParms == null)
                return;
            
            try
            {
                int i = 0;

                foreach (DataRow dr in dtParms.Rows)
                {
                    if (dr["DATA_TYPE"].ToString() == "INT")
                        sqlcmd.Parameters.AddWithValue("@" + dr["PARM_NAME"], Convert.ToInt32(drValue[i].ToString()));

                    if (dr["DATA_TYPE"].ToString() == "STRING")
                        sqlcmd.Parameters.AddWithValue("@" + dr["PARM_NAME"], drValue[i].ToString());

                    if (dr["DATA_TYPE"].ToString() == "BIT")
                        sqlcmd.Parameters.AddWithValue("@" + dr["PARM_NAME"], Convert.ToBoolean(drValue[i].ToString()));

                    if (dr["DATA_TYPE"].ToString() == "FLOAT")
                        sqlcmd.Parameters.AddWithValue("@" + dr["PARM_NAME"], Convert.ToSingle(drValue[i].ToString()));

                    if (dr["DATA_TYPE"].ToString() == "DATETIME")                    
                        sqlcmd.Parameters.AddWithValue("@" + dr["PARM_NAME"], Convert.ToDateTime(drValue[i].ToString()));                                            

                    if (dr["DATA_TYPE"].ToString() == "BIGINT")
                        sqlcmd.Parameters.AddWithValue("@" + dr["PARM_NAME"], Convert.ToInt64(drValue[i].ToString()));

                    if (dr["DATA_TYPE"].ToString() == "TINYINT")
                        sqlcmd.Parameters.AddWithValue("@" + dr["PARM_NAME"], Convert.ToInt16(drValue[i].ToString()));

                    i++;
                }
            }
            catch (Exception ex)
            {
                
            }

        }

        
        private DataTable RunSelectSP(DataTable dtParms, string strSPName)
        {
            
            DataTable dtResult = new DataTable();
            string[] strPramDetails = new string[2];
            string strConnectionString = strSPConnectionString;
            
            using (SqlConnection sqlconn = new SqlConnection(strConnectionString))
            {
                try
                {
                    SqlDataAdapter da = new SqlDataAdapter();
                    da.SelectCommand = new SqlCommand(strSPName, sqlconn);
                    da.SelectCommand.CommandType = CommandType.StoredProcedure;

                    AddSingleParameters(da.SelectCommand, dtParms);                   

                    da.Fill(dtResult);

                    return dtResult;
                }
                catch (Exception ex)
                {
                    TestLogFiles("GeneralDBOp - Select SP Failed. - " + strSPName + "," + ex.Message);
                    return null;
                }
                finally
                {
                    sqlconn.Close();
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
                    TestLogFiles("GeneralDBOp - Select Ad-hoc Failed. - " + QueryString + "," + ex.Message);
                    return null;
                }
                finally
                {
                    sqlconn.Close();
                }
            }
        }


        private void AddSingleParameters(SqlCommand sqlcmd, DataTable dtParms)
        {

            if (dtParms == null)
                return;

            try
            {
                foreach (DataRow dr in dtParms.Rows)
                {
                    if (dr["DATA_TYPE"].ToString() == "INT")
                        sqlcmd.Parameters.AddWithValue("@" + dr["PARM_NAME"], Convert.ToInt32(dr["DATA_VALUE"].ToString()));

                    if (dr["DATA_TYPE"].ToString() == "STRING")
                        sqlcmd.Parameters.AddWithValue("@" + dr["PARM_NAME"], dr["DATA_VALUE"].ToString());

                    if (dr["DATA_TYPE"].ToString() == "BIT")
                        sqlcmd.Parameters.AddWithValue("@" + dr["PARM_NAME"], Convert.ToBoolean(dr["DATA_VALUE"].ToString()));

                    if (dr["DATA_TYPE"].ToString() == "FLOAT")
                    {
                        sqlcmd.Parameters.AddWithValue("@" + dr["PARM_NAME"], Convert.ToSingle(dr["DATA_VALUE"].ToString()));
                    }
                    if (dr["DATA_TYPE"].ToString() == "DATETIME")
                        sqlcmd.Parameters.AddWithValue("@" + dr["PARM_NAME"], Convert.ToDateTime(dr["DATA_VALUE"].ToString()));

                    if (dr["DATA_TYPE"].ToString() == "BIGINT")
                        sqlcmd.Parameters.AddWithValue("@" + dr["PARM_NAME"], Convert.ToInt64(dr["DATA_VALUE"].ToString()));

                    if (dr["DATA_TYPE"].ToString() == "TINYINT")
                        sqlcmd.Parameters.AddWithValue("@" + dr["PARM_NAME"], Convert.ToInt16(dr["DATA_VALUE"].ToString()));


                }
            }
            catch (Exception ex)
            {
                TestLogFiles("AddSingleParameters failed - " + ex.Message);
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

        private int GetServerNumber(DataTable dtParms)
        {
            if(dtParms == null)
                return 0;

            try
            {
                foreach (DataRow dr in dtParms.Rows)
                {
                    iServerNumber = Convert.ToInt32(dr["SERVER_NUMBER"].ToString());
                    return iServerNumber;
                }
            }
            catch (Exception ex)
            {                
            }

            return 0;

        }

        private DataTable SetParmeterTable()
        {
            DataTable dtParms = new DataTable();

            dtParms.Columns.Add(new DataColumn("SERVER_NUMBER", typeof(int)));
            dtParms.Columns.Add(new DataColumn("PARM_NAME", typeof(string)));
            dtParms.Columns.Add(new DataColumn("DATA_TYPE", typeof(string)));
            dtParms.Columns.Add(new DataColumn("DATA_VALUE", typeof(string)));

            return dtParms;
        }
    }
}
