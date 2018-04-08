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
    /// PerfValueInsert의 요약 설명입니다.
    /// </summary>
    [WebService(Namespace = "http://sqlmvp.kr")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // ASP.NET AJAX를 사용하여 스크립트에서 이 웹 서비스를 호출하려면 다음 줄의 주석 처리를 제거합니다. 
    // [System.Web.Script.Services.ScriptService]
    public class PerfValueInsert : System.Web.Services.WebService
    {
        string strSPConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings["SPConnectionString"].ConnectionString;

        [WebMethod]
        public int PValueInsert(Byte[] btValues, int iServerNumber, string strTimeIn, string strTimeIn_UTC)
        {
            DataTable dtValues = new DataTable();

            if (btValues == null)
                return 0;

            dtValues = BytesToDataTable(btValues);
            if (dtValues == null)
                return 0;

            //tbDashboard AD-HOC JSON 인서트
            string jsonString = JsonConvert.SerializeObject(dtValues);
            //System.Diagnostics.Debug.WriteLine(jsonString);

            try
            {
                string strQuery = @"insert into tbPerfmonValues_JSON (TimeIn_UTC, ServerNum, Data_JSON) values (N'" + strTimeIn_UTC + "', N'" + iServerNumber.ToString() + "', N'" + jsonString + "')";
                RunQueryCommand(strQuery);

            }
            catch (Exception ex)
            {
                TestLogFiles("Perf Insert Failed - InsertPerfdataUsingAdHoc Error : " + ex.Message);
            }

            //tbDashboard AD-HOC 인서트
            Insert_tbDashboard_AdHoc(dtValues, iServerNumber, strTimeIn, strTimeIn_UTC);

            return dtValues.Rows.Count;
        }
        

        void Insert_tbDashboard_AdHoc(DataTable dtPValues, int iServerNumber, string strTimeIn, string strTimeIn_UTC)
        {
            DataTable dtDashboard = new DataTable();
            dtDashboard.Columns.Add("TimeIn");
            dtDashboard.Columns.Add("ServerNum");
            dtDashboard.Columns.Add("PNum");
            dtDashboard.Columns.Add("PValue");


            try
            {
                //             ('2015-10-08 06:53:22.887', '2015-10-08 06:53:07.000', 2152, 'P158', 4096, '0', 'C:'), 
                //             ('2015-10-08 06:53:22.887', '2015-10-08 06:53:07.000', 2152, 'P158', 4096, '0', 'C:')

                /*
                             열1	PCID	Perf Object	Perf Counter	Instance	Fomula	비고
                P0	P001	Processor	% Processor Time	_Total		%CPU Time
                P1	P002	Processor	% Privileged time	_Total		Kernel CPU
                P2	P004	System	Processor Queue Length	FALSE		queue length
                P3	P007	Memory	Available Mbytes	FALSE		Available
                P4	P008	Memory	Committed bytes	FALSE		Committed
                P5	P015	LogicalDisk	% Disk Time	_Total		
                P6	P016	LogicalDisk	Avg. Disk sec/Read	_Total		Read Time
                P7	P018	LogicalDisk	Free Megabytes	C:		Free Space
                P8	P020	Network Interface	Bytes total/sec	*	Max	Network Bytes Total/sec
                P9	P170	Network Interface	Bytes Received/sec	*	Max	
                P10	P171	Network Interface	Bytes Sent/sec	*	Max	
                P11	P022	Web Service	Bytes Total/sec	_Total		WWW Service Total Bytes/sec
                P12	P023	Web Service	Current connections	_Total		Total Connections
                P13	P006	Process	% Processor time	W3WP#	Sum/#CPU	%CPU(W3WP)
                P14	P013	Process	Private Bytes	W3WP#	Max	Worker Process Memory (Max)
                P15	P013	Process	Private Bytes	W3WP#	Sum	Worker Process Memory (Total)
                P16	P035, P041, P047, P027	ASP/ASP.NET	Request Execution Time	*	Max	Req Execution Time
                P17	P036, P042, P048, P028	ASP/ASP.NET	Requests Executing	*	Max	Req Executing
                P18	P053, P054, P029	ASP/ASP.NET	Requests Queued	*	Max	Req Queued
                P19	P039, P045, P051, P030	ASP/ASP.NET	Requests/Sec	*	Max	Req/sec
                P20	P139	SQLServer:Resource pool Stats	CPU Usage %	default		
                P21	P100	SQLServer:Memory Manager	Target Server Memory (KB)	FALSE		Used
                P22	P101	SQLServer:Memory Manager	Total Server Memory (KB)	FALSE		Allocated
                P23	P157	PhysicalDisk	% Idle Time	_Total		
                P24	P155	PhysicalDisk	% Disk Time	_Total		
                P25	P161	PhysicalDisk	Avg. Disk Queue Length	_Total		
                P26	P081	SQLServer:Buffer Manager	Buffer cache hit ratio	FALSE		
                P27	P138	SQLServer:Plan Cache	Cache Hit Ratio	_Total		
                P28	P106	SQLServer:SQL Statistics	Batch Requests/sec	FALSE		
                P29	P107	SQLServer:SQL Statistics	SQL Compilations/sec	FALSE		
                P30	P108	SQLServer:SQL Statistics	SQL Re-Compilations/sec	FALSE		
                P31	P084	SQLServer:Buffer Manager	Page life expectancy	FALSE		
                P32	P090	SQLServer:Buffer Manager	Database Pages	FALSE		
                P33	P091	SQLServer:Plan Cache	Cache Pages	FALSE		
                P34	P082	SQLServer:Buffer Manager	Checkpoint pages/sec	FALSE		
                P35	P200	PhysicalDisk	Disk Reads/sec	_Total		
                P36	P201	PhysicalDisk	Disk Write/sec	_Total		
                P37	P088	SQLServer:Buffer Manager	Readahead pages/sec	FALSE		
                P38	P083	SQLServer:Buffer Manager	Lazy writes/sec	FALSE		
                P39	P119	SQLServer:Databases	Log Flushes/sec	_Total		
                P40	P098	SQLServer:General Statistics	User Connections	FALSE		
                P41	P115	SQLServer:Databases	Data File(s) Size (KB)	_Total		
                P42	P116	SQLServer:Databases	Log File(s) Size (KB)	_Total		
                P43	P117	SQLServer:Databases	Log File(s) Used Size (KB)	_Total		
                P44 ReponseTime / select 1
                */

                string strRecord = "";
                
                float[] strValue = new float[53];

                string strPCID;
                string strInstance;
                float flPValue;

                float flSaved8 = 0;
                float flSaved9 = 0;
                float flSaved10 = 0;
                float flSaved14 = 0;
                float flSaved16 = 0;
                float flSaved17 = 0;
                float flSaved18 = 0;
                float flSaved19 = 0;
                float flSaved45 = 0;

                foreach (DataRow dr in dtPValues.Rows)
                {
                    strPCID = dr["PCID"].ToString();
                    strInstance = dr["InstanceName"].ToString();
                    flPValue = Convert.ToSingle(dr["PValue"]);
                    
                    //v3. P001, P002 값이 100보다 크면, '0'으로 설정.
                    if (strPCID == "P001" && strInstance.Contains("_Total"))
                    {
                        if (flPValue > 100)
                            strValue[0] = 0;
                        else
                            strValue[0] = flPValue;
                    }

                    if (strPCID == "P002" && strInstance.Contains("_Total"))
                    {
                        if (flPValue > 100)
                            strValue[1] = 0;
                        else
                            strValue[1] = flPValue;
                    }

                    if (strPCID == "P004")
                        strValue[2] = flPValue;

                    if (strPCID == "P007")
                        strValue[3] = flPValue;

                    if (strPCID == "P008")
                        strValue[4] = flPValue;

                    if (strPCID == "P015" && strInstance.Contains("_Total"))
                        strValue[5] = flPValue;

                    if (strPCID == "P016" && strInstance.Contains("_Total"))
                        strValue[6] = flPValue;

                    if (strPCID == "P018" && strInstance.Contains("C:"))
                        strValue[7] = flPValue;

                    if (strPCID == "P020")      // To save MAX value
                    {
                        strValue[8] = flPValue;
                        if (flPValue < flSaved8)
                            strValue[8] = flSaved8;
                        flSaved8 = strValue[8];

                    }

                    if (strPCID == "P170")      // To save MAX value
                    {
                        strValue[9] = flPValue;
                        if (flPValue < flSaved9)
                            strValue[9] = flSaved9;
                        flSaved9 = strValue[9];
                    }

                    if (strPCID == "P171")      // To save MAX value
                    {
                        strValue[10] = flPValue;
                        if (flPValue < flSaved10)
                            strValue[10] = flSaved10;
                        flSaved10 = strValue[10];
                    }

                    if (strPCID == "P022" && strInstance.Contains("_Total"))
                        strValue[11] = flPValue;

                    if (strPCID == "P023" && strInstance.Contains("_Total"))
                        strValue[12] = flPValue;

                    if (strPCID == "P006" && strInstance.Contains("w3wp"))
                        //dr["P13"] += (flPValue / g_SharedData.SYSINFO.iNumberOfProcessors);
                        strValue[13] = flPValue;

                    if (strPCID == "P013" && strInstance.Contains("w3wp"))      // Max W3WP Memory, Total W3WP Memory
                    {
                        strValue[14] = flPValue;
                        if (flPValue < flSaved14)
                            strValue[14] = flSaved14;
                        flSaved14 = strValue[14];

                        strValue[15] = flPValue;
                    }

                    if (strPCID == "P035" || strPCID == "P041" || strPCID == "P047" || strPCID == "P027")
                    {
                        strValue[16] = flPValue;
                        if (flPValue < flSaved16)
                            strValue[16] = flSaved16;
                        flSaved16 = strValue[16];
                    }

                    if (strPCID == "P036" || strPCID == "P042" || strPCID == "P048" || strPCID == "P028")
                    {
                        strValue[17] = flPValue;
                        if (flPValue < flSaved17)
                            strValue[17] = flSaved17;
                        flSaved17 = strValue[17];
                    }

                    if (strPCID == "P053" || strPCID == "P054" || strPCID == "P029")
                    {
                        strValue[18] = flPValue;
                        if (flPValue < flSaved18)
                            strValue[18] = flSaved18;
                        flSaved18 = strValue[18];
                    }

                    if (strPCID == "P039" || strPCID == "P045" || strPCID == "P051" || strPCID == "P030")
                    {
                        strValue[19] = flPValue;
                        if (flPValue < flSaved19)
                            strValue[19] = flSaved19;
                        flSaved19 = strValue[19];
                    }

                    if (strPCID == "P139" && strInstance.Contains("default"))
                        strValue[20] = flPValue;

                    if (strPCID == "P100")
                        strValue[21] = flPValue;

                    if (strPCID == "P101")
                        strValue[22] = flPValue;

                    if (strPCID == "P157" && strInstance.Contains("_Total"))
                        strValue[23] = flPValue;

                    if (strPCID == "P155" && strInstance.Contains("_Total"))
                        strValue[24] = flPValue;

                    if (strPCID == "P161" && strInstance.Contains("_Total"))
                        strValue[25] = flPValue;

                    if (strPCID == "P081")
                        strValue[26] = flPValue;

                    if (strPCID == "P138" && strInstance.Contains("_Total"))
                        strValue[27] = flPValue;

                    if (strPCID == "P106")
                        strValue[28] = flPValue;

                    if (strPCID == "P107")
                        strValue[29] = flPValue;

                    if (strPCID == "P108")
                        strValue[30] = flPValue;

                    if (strPCID == "P084")
                        strValue[31] = flPValue;

                    if (strPCID == "P090")
                        strValue[32] = flPValue;

                    if (strPCID == "P091")
                        strValue[33] = flPValue;

                    if (strPCID == "P082")
                        strValue[34] = flPValue;

                    if (strPCID == "P200" && strInstance.Contains("_Total"))
                        strValue[35] = flPValue;

                    if (strPCID == "P201" && strInstance.Contains("_Total"))
                        strValue[36] = flPValue;

                    if (strPCID == "P088")
                        strValue[37] = flPValue;

                    if (strPCID == "P083")
                        strValue[38] = flPValue;

                    if (strPCID == "P119" && strInstance.Contains("_Total"))
                        strValue[39] = flPValue;

                    if (strPCID == "P098")
                        strValue[40] = flPValue;

                    if (strPCID == "P115" && strInstance.Contains("_Total"))
                        strValue[41] = flPValue;

                    if (strPCID == "P116" && strInstance.Contains("_Total"))
                        strValue[42] = flPValue;

                    if (strPCID == "P117" && strInstance.Contains("_Total"))
                        strValue[43] = flPValue;

                    //HC QueryTime
                    if (strPCID == "D001")
                    {
                        strValue[44] = flPValue;
                    }

                    // Output Queue Length 를 P45에 추가함.
                    if (strPCID == "P021")      // To save MAX value
                    {
                        strValue[45] = flPValue;
                        if (flPValue < flSaved45)
                            strValue[45] = flSaved45;
                        flSaved45 = strValue[45];
                    }

                    if (strPCID == "P099")
                        strValue[46] = flPValue;

                    if (strPCID == "P114" && strInstance.Contains("_Total"))
                        strValue[47] = flPValue;

                    if (strPCID == "P202" && strInstance.Contains("_Total"))
                        strValue[48] = flPValue;

                    if (strPCID == "P203" && strInstance.Contains("_Total"))
                        strValue[49] = flPValue;

                    if (strPCID == "P204" || strPCID == "P205")
                    {
                        strValue[50] = flPValue;
                    }

                    // P51 is reserved.
                    strValue[51] = 0;

                    if (strPCID == "P033" || strPCID == "P034")
                    {
                        strValue[52] = flPValue;
                    }

                }

                strRecord = " ('" + strTimeIn_UTC + "', '" + strTimeIn + "', " + iServerNumber.ToString() + ", '"
                    + strValue[0].ToString() + "', " + strValue[1].ToString() + ", " + strValue[2].ToString() + ", " + strValue[3].ToString() + ", " + strValue[4].ToString() + ", " + strValue[5].ToString() + ", " + strValue[6].ToString() + ", " + strValue[7].ToString() + ", " + strValue[8].ToString() + ", " + strValue[9].ToString() + ", " + strValue[10].ToString() + ", "
                    + strValue[11].ToString() + ", " + strValue[12].ToString() + ", " + strValue[13].ToString() + ", " + strValue[14].ToString() + ", " + strValue[15].ToString() + ", " + strValue[16].ToString() + ", " + strValue[17].ToString() + ", " + strValue[18].ToString() + ", " + strValue[19].ToString() + ", " + strValue[20].ToString() + ", "
                    + strValue[21].ToString() + ", " + strValue[22].ToString() + ", " + strValue[23].ToString() + ", " + strValue[24].ToString() + ", " + strValue[25].ToString() + ", " + strValue[26].ToString() + ", " + strValue[27].ToString() + ", " + strValue[28].ToString() + ", " + strValue[29].ToString() + ", " + strValue[30].ToString() + ", "
                    + strValue[31].ToString() + ", " + strValue[32].ToString() + ", " + strValue[33].ToString() + ", " + strValue[34].ToString() + ", " + strValue[35].ToString() + ", " + strValue[36].ToString() + ", " + strValue[37].ToString() + ", " + strValue[38].ToString() + ", " + strValue[39].ToString() + ", " + strValue[40].ToString() + ", "
                    + strValue[41].ToString() + ", " + strValue[42].ToString() + ", " + strValue[43].ToString() + ", " + strValue[44].ToString() + ", " + strValue[45].ToString() + ", " + strValue[46].ToString() + ", " + strValue[47].ToString() + ", " + strValue[48].ToString() + ", " + strValue[49].ToString() + ", " + strValue[50].ToString() + ", "
                    + strValue[51].ToString() + ", " + strValue[52].ToString() + ")";

                string strQuery = @"insert into tbDashboard (TimeIn_UTC, TimeIn, ServerNum, P0, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11, P12, P13, P14, P15, P16, P17, P18, P19, P20, P21, P22,P23,P24, P25, P26, P27, P28 , P29, P30, P31, P32, P33, P34, P35, P36, P37, P38, P39, P40, P41, P42, P43, P44, P45, P46, P47, P48, P49, P50, P51 , P52) values ";
                strQuery += strRecord;

                RunQueryCommand(strQuery);

                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P0", strValue[0].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P1", strValue[1].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P2", strValue[2].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P3", strValue[3].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P4", strValue[4].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P5", strValue[5].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P6", strValue[6].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P7", strValue[7].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P8", strValue[8].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P9", strValue[9].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P10", strValue[10].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P11", strValue[11].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P12", strValue[12].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P13", strValue[13].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P14", strValue[14].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P15", strValue[15].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P16", strValue[16].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P17", strValue[17].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P18", strValue[18].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P19", strValue[19].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P20", strValue[20].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P21", strValue[21].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P22", strValue[22].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P23", strValue[23].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P24", strValue[24].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P25", strValue[25].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P26", strValue[26].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P27", strValue[27].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P28", strValue[28].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P29", strValue[29].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P30", strValue[30].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P31", strValue[31].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P32", strValue[32].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P33", strValue[33].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P34", strValue[34].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P35", strValue[35].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P36", strValue[36].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P37", strValue[37].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P38", strValue[38].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P39", strValue[39].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P40", strValue[40].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P41", strValue[41].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P42", strValue[42].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P43", strValue[43].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P44", strValue[44].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P45", strValue[45].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P46", strValue[46].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P47", strValue[47].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P48", strValue[48].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P49", strValue[49].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P50", strValue[50].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P51", strValue[51].ToString());
                dtDashboard.Rows.Add( strTimeIn, iServerNumber.ToString(), "P52", strValue[52].ToString());

                //tbDashboard AD-HOC JSON 인서트
                string jsonString = JsonConvert.SerializeObject(dtDashboard);
                //System.Diagnostics.Debug.WriteLine(jsonString);

                strQuery = @"insert into tbDashboard_JSON (TimeIn_UTC, ServerNum, Data_JSON) values (N'" + strTimeIn_UTC + "', N'" + iServerNumber.ToString() + "', N'" + jsonString + "')";
                RunQueryCommand(strQuery);

            }
            catch (Exception ex)
            {
                TestLogFiles("Perf Insert Failed - Insert_tbDashboard_AdHoc Error : " + ex.Message);
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
                    TestLogFiles("Perf Insert RunQueryCommand Error - " + ex.Message + " : " + strQuery);
                }
            }
        }


        private string GetConnectionString(int iServerNumber)
        {
            strSPConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings["SPConnectionString"].ConnectionString;
            return strSPConnectionString;
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
