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
    /// GetDBDataSample의 요약 설명입니다.
    /// </summary>
    [WebService(Namespace = "http://tempuri.org/")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // ASP.NET AJAX를 사용하여 스크립트에서 이 웹 서비스를 호출하려면 다음 줄의 주석 처리를 제거합니다. 
    // [System.Web.Script.Services.ScriptService]
    public class GetDBDataSample : System.Web.Services.WebService
    {

        [WebMethod]
        public string HelloWorld()
        {
            return "Hello World";
        }

        [WebMethod]
        public int Send_TableDataToInsert(Byte[] bytearr, int YBIS_CHK_KEY)
        {
            DataTable dtYBIS_Sample = new DataTable();

            if (bytearr == null)
                return 0;

            // 전송 받은 바이트 데이터를 데이터테이블로 변환한다.
            dtYBIS_Sample = BytesToDataTable(bytearr);

            if (dtYBIS_Sample == null)
                return 0;

            // 데이터 테이블에 있는 데이터를 파일로 기록한다.
            RecordReceivedData(dtYBIS_Sample);

            return dtYBIS_Sample.Rows.Count;

        }

        private void RecordReceivedData(DataTable dtSample)
        {
            string strRecord = "";

            if (dtSample.Rows.Count < 1)
            {
                TestLogFiles("No data to write");
                return;
            }

            foreach (DataColumn col in dtSample.Columns)
            {
                strRecord += col.ColumnName + ",";
            }

            TestLogFiles(strRecord);

            int iCol = dtSample.Columns.Count;

            foreach (DataRow dr in dtSample.Rows)
            {
                for (int i = 0; i < iCol; i++)
                {
                    strRecord += dr[i].ToString() + ",";
                }

                TestLogFiles(strRecord);
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

        void TestLogFiles(string strLogMsg)
        {

            string strPath = @"F:\\ServiceLog\\TableDataFromClient.txt";

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


    }
}
