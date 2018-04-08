﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Services;

namespace SimpleWebService
{
    /// <summary>
    /// Adder의 요약 설명입니다.
    /// </summary>
    [WebService(Namespace = "http://sqlmvp.kr")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // ASP.NET AJAX를 사용하여 스크립트에서 이 웹 서비스를 호출하려면 다음 줄의 주석 처리를 제거합니다. 
    // [System.Web.Script.Services.ScriptService]
    public class Adder : System.Web.Services.WebService
    {

        [WebMethod]
        public int SumAB(int a, int b)
        {
            return a+b;
        }
    }
}
