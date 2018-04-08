using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.IO;

namespace SimpleWebService
{
    class Util
    {
        public static byte[] func_DataTableToByte(DataTable dt)
        {
            try
            {
                MemoryStream stream = new MemoryStream();
                System.Runtime.Serialization.IFormatter formatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
                formatter.Serialize(stream, dt);
                return stream.GetBuffer();
            }
            catch (Exception e)
            {
                return new byte[0];
            }
        }
        public static DataTable func_ByteToDataTable(byte[] bytearray)
        {
            try
            {
                System.IO.MemoryStream stream = new MemoryStream(bytearray);
                System.Runtime.Serialization.IFormatter formatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
                DataTable dt = (DataTable)formatter.Deserialize(stream);

                //ds.ReadXml(sr);
                if (dt == null)
                {
                    return new DataTable();
                }
                return dt;
            }
            catch (Exception ex)
            {
                return new DataTable();
            }

        }
    }
}
