using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;

namespace MysqlDumpBinlog
{
    class Program
    {
        static void Main(string[] args)
        {
            var buffer = new byte[1024];
            Socket mysqlConn = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            mysqlConn.Connect(new IPEndPoint(IPAddress.Loopback, 3306));
            int i = mysqlConn.Receive(buffer);
            if (i == 0)
                return;
            var req = new ServerReq(buffer.Take(i).ToArray());
            req.StartParse();
            var resp = new Response("root", "mikawudi", req.AuthData.Take(20).ToArray(), "db1");
            var sendBuf = resp.toArray();
            mysqlConn.Send(sendBuf);
            i = mysqlConn.Receive(buffer);
            if (i == 0)
                return;
            var logInfo = buffer.Take(i).ToArray();
            ParseLoginResult(logInfo);

            //ping测试
            var pingbuf = new List<byte>();
            pingbuf.Add(0x0e);
            pingbuf.InsertRange(0, BitConverter.GetBytes(1));
            mysqlConn.Send(pingbuf.ToArray());
            i = mysqlConn.Receive(buffer);
            var resp2 = buffer.Take(i).ToArray();
            Console.WriteLine(resp2[0]);
            //进入binlog模式
            EntryBinLogModel(mysqlConn, "mysqlbin.000013", 4, new byte[] {0x00, 0x00}, 1090);


            return;
            var query = new List<byte>();
            query.Add(0x03);
            query.AddRange(Encoding.ASCII.GetBytes("select * from company order by CreateTime desc limit 5"));
            query.InsertRange(0, BitConverter.GetBytes(query.Count));
            query[3] = 0x00;
            mysqlConn.Send(query.ToArray());
            i = mysqlConn.Receive(buffer);
            int packLeng = 0;
            buffer = buffer.Take(i).ToArray();
            //Result Set!

            //Result Set Header 
            int colCount = parseCount(buffer, ref packLeng);
            buffer = buffer.Skip(packLeng).ToArray();
            packLeng = 0;
            //Field (maybe Mul)
            var colDefineInfo = ParseColDefine(buffer, ref packLeng);
            buffer = buffer.Skip(packLeng).ToArray();
            //EOF
            var eoflen = parseEOF(buffer);
            buffer = buffer.Skip(eoflen).ToArray();
            //ROW DATA (maybe Mul)
            ReadResultData(ref buffer, colCount);
            //EOF(也有可能是OK)
            eoflen = parseEOF(buffer);
            buffer = buffer.Skip(eoflen).ToArray();
        }

        static void EntryBinLogModel(Socket socket, string binlogName, int position, byte[] flag, int sliveServerId)
        {
            var recvBuffer = new byte[4096];
            //查询master的全局binlog_checksum选项
            var query = new List<byte>();
            query.Add(0x03);
            query.AddRange(Encoding.ASCII.GetBytes("select @@global.binlog_checksum"));
            query.InsertRange(0, BitConverter.GetBytes(query.Count));
            query[3] = 0x00;
            socket.Send(query.ToArray());
            var recLength = socket.Receive(recvBuffer, 0, recvBuffer.Length, SocketFlags.None);
            List<Tuple<byte, byte[]>> packlist = new List<Tuple<byte, byte[]>>();
            var recvPack = recvBuffer.Take(recLength).ToArray();
            while (recvPack.Length > 0)
            {
                byte seq = recvPack[3];
                recvPack[3] = 0;
                var length = BitConverter.ToInt32(recvPack, 0);
                var tuple = new Tuple<byte, byte[]>(seq, recvPack.Skip(4).Take(length).ToArray());
                packlist.Add(tuple);
                recvPack = recvPack.Skip(length + 4).ToArray();
            }
            var resultSet = new ResultSet();
            foreach (var pack in packlist)
            {
                resultSet.AddData(pack.Item1, pack.Item2);
            }
            packlist.Clear();
            var str = resultSet.GetData(0, 0);
            if (str == "CRC32")
            {
                var query2 = new List<byte>();
                query2.Add(0x03);
                query2.AddRange(Encoding.ASCII.GetBytes("SET @master_binlog_checksum= 'CRC32'"));
                query2.InsertRange(0, BitConverter.GetBytes(query2.Count));
                query2[3] = 0x00;
                socket.Send(query2.ToArray());
                recLength = socket.Receive(recvBuffer, 0, recvBuffer.Length, SocketFlags.None);
                recvPack = recvBuffer.Take(recLength).ToArray();
                while (recvPack.Length > 0)
                {
                    byte seq = recvPack[3];
                    recvPack[3] = 0;
                    var length = BitConverter.ToInt32(recvPack, 0);
                    var tuple = new Tuple<byte, byte[]>(seq, recvPack.Skip(4).Take(length).ToArray());
                    packlist.Add(tuple);
                    recvPack = recvPack.Skip(length + 4).ToArray();
                }
                //OK 响应报文
                if (!(packlist.Count == 1 && packlist[0].Item2[0] == 0x00))
                {
                    throw new Exception("set master_binlog_checksum error!");
                }
            }


            var binlog_dumpPack = new List<byte>();
            //command
            binlog_dumpPack.Add(0x12);
            binlog_dumpPack.AddRange(BitConverter.GetBytes(position));
            binlog_dumpPack.AddRange(flag);
            binlog_dumpPack.AddRange(BitConverter.GetBytes(sliveServerId));
            binlog_dumpPack.AddRange(Encoding.ASCII.GetBytes(binlogName));
            binlog_dumpPack.InsertRange(0, BitConverter.GetBytes(binlog_dumpPack.Count));
            //seq
            binlog_dumpPack[3] = 0x00;

            socket.Send(binlog_dumpPack.ToArray());
            List<Tuple<byte, byte[]>> binlogEvent = new List<Tuple<byte, byte[]>>();
            List<byte> dataList = new List<byte>();
            Queue<byte[]> packetQueue = new Queue<byte[]>();
            byte preseq = 0x00;
            //int status = 0;// 0: 未读取OK, 1:读取OK未读取FORMAT_DESCRIPTION_EVENT, 2:正常工作状态
            Func<byte[], Tuple<bool, string>> action = ParseOK;
            while (true)
            {
                var realRead = socket.Receive(recvBuffer, 0, recvBuffer.Length, SocketFlags.None);
                if (realRead == 0)
                    break;
                dataList.AddRange(recvBuffer.Take(realRead).ToArray());
                while (dataList.Count > 4)
                {
                    if(dataList[3] != (preseq+1))
                        throw new Exception("seq error");
                    preseq++;
                    dataList[3] = 0x00;
                    var length = BitConverter.ToInt32(dataList.Take(4).ToArray(), 0);
                    if (dataList.Count < length + 4)
                    {
                        break;
                    }
                    packetQueue.Enqueue(dataList.Skip(4).Take(length).ToArray());
                    dataList.RemoveRange(0, 4 + length);
                }
                while (packetQueue.Count > 0)
                {
                    var data = packetQueue.Dequeue();
                    var result = action(data);
                    if(!result.Item1)
                        throw new Exception(result.Item2);
                    if (action == ParseOK)
                    {
                        action = ParseFormat_Description;
                        continue;
                    }
                    if (action == ParseFormat_Description)
                    {
                        action = ParseEventData;
                    }
                }
            }
            socket.Close();
        }

        static Tuple<bool, string> ParseOK(byte[] data)
        {
            if(data[0] == 0x00)
                return new Tuple<bool, string>(true, null);
            return new Tuple<bool, string>(false, null);
        }

        private static byte[] post_header_length;
        private static int binlogVersion = 0;
        /// <summary>
        /// 解析format post-header-length
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        static Tuple<bool, string> ParseFormat_Description(byte[] data)
        {
            var marker = data[0];
            //跳过marker,获取eventheader
            var head = data.Skip(1).Take(19).ToArray();
            var timestamp = BitConverter.ToUInt32(head, 0);
            var times = new DateTime(1970, 1, 1).AddSeconds(timestamp).ToString("yyyy-MM-dd HH:mm:ss");
            var eventType = head[4];
            var serverid = BitConverter.ToInt32(head, 5);
            var eventLength = BitConverter.ToInt32(head, 9);
            var nexPosition = BitConverter.ToInt32(head, 13);
            var flag = head.Skip(17).Take(2).ToArray();
            var databody = data.Skip(20).ToArray();
            if (databody.Length != eventLength - 19)
                throw new Exception();
            if(eventType != 0x0f)
                throw new Exception();
            var foemate = FORMAT_DESCRIPTION_EVENT.Create(databody);
            post_header_length = foemate.eventTypeHeaderLength; //私有headerlength的map
            if(foemate.version != 4)
                throw new Exception();
            binlogVersion = foemate.version;
            return new Tuple<bool, string>(true, null);
        }

        /// <summary>
        /// 解析普通Event
        /// </summary>
        /// <param name="data"></param>
        static Tuple<bool, string> ParseEventData(byte[] data)
        {
            var marker = data[0];
            //跳过marker,获取eventheader
            var head = data.Skip(1).Take(19).ToArray();
            var timestamp = BitConverter.ToUInt32(head, 0);
            var times = new DateTime(1970, 1, 1).AddSeconds(timestamp).ToString("yyyy-MM-dd HH:mm:ss");
            var eventType = head[4];
            var serverid = BitConverter.ToInt32(head, 5);
            var eventLength = BitConverter.ToInt32(head, 9);
            var nexPosition = BitConverter.ToInt32(head, 13);
            var flag = head.Skip(17).Take(2).ToArray();
            var databody = data.Skip(20).ToArray();
            if (databody.Length != eventLength - 19)
            {
                throw new Exception();
            }
            switch (eventType)
            {
                case 0x00:
                    return new Tuple<bool, string>(false, "unknow message");
                case 0x02:
                    ParseQuery(databody);
                    return new Tuple<bool, string>(true, "");
                case 0x13:
                    ParseTableMapEvent(databody);
                    return new Tuple<bool, string>(true, "");
                case 0x0f:
                    throw new Exception();
                    //Console.WriteLine(FORMAT_DESCRIPTION_EVENT.Create(databody));
                default:
                    return new Tuple<bool, string>(true, "uncare");
            }
            return new Tuple<bool, string>(true, null);
        }
        static Dictionary<int, int> col_meta_def = new Dictionary<int, int>()
        {

        };
        static void ParseTableMapEvent(byte[] data)
        {
            byte id = 0x13;
            var tablemaperpostlen = post_header_length[id];
            byte[] tableid;
            var tablelen = 0;
            if (tablemaperpostlen == 6)
            {
                tablelen = 4;
            }
            else
            {
                tablelen = 6;
            }
            tableid = data.Take(tablelen).ToArray();
            var flag = data.Skip(tablelen).Take(2).ToArray();
            var index = tablelen + 2;
            var schemanamelen = data[index];
            index++;
            var schemaname = Encoding.UTF8.GetString(data, index, schemanamelen);
            index += (schemanamelen + 1);
            var tablenamelen = data[index];
            index++;
            var tablename = Encoding.UTF8.GetString(data, index, tablenamelen);
            index += (tablenamelen + 1);
            var temp = data.Skip(index).ToArray();
            byte lenlen = 0;
            var colcount = getStrLeng(temp, ref lenlen);
            index += lenlen;
            var colTypeDef = data.Skip(index).Take((int)colcount).ToArray();
            index += (int)colcount;
            List<>

        }
        public class MyTuple<T1, T2>
        {
            public 
        }
        static void ParseQuery(byte[] bodydata)
        {
            byte id = 0x02;
            if(post_header_length[id] != 13)
                throw new Exception();
            var post_header = bodydata.Take(13).ToArray();
            var slave_proxy_id = BitConverter.ToUInt32(post_header, 0);
            var execute_time = BitConverter.ToUInt32(post_header, 4);
            var scheme_length = post_header[8];
            var error_code = BitConverter.ToInt16(post_header, 9);
            var status_vars_length = BitConverter.ToInt16(post_header, 11);

            var status_vars = Encoding.UTF8.GetString(bodydata.Skip(13).Take(status_vars_length).ToArray());
            var schema = Encoding.UTF8.GetString(bodydata.Skip(13 + status_vars_length).Take(scheme_length).ToArray());
            var spilt = bodydata[13 + status_vars_length + scheme_length];
            var len = bodydata.Length - (13 + status_vars_length + scheme_length + 1) - 4;
            var query = Encoding.UTF8.GetString(bodydata.Skip(13 + status_vars_length + scheme_length + 1).Take(len).ToArray());
            var crc = bodydata.Skip(bodydata.Length - 4).ToArray();
        }

        static List<List<string>> ReadResultData(ref byte[] buffer, int colCount)
        {
            List<List<string>> result = new List<List<string>>();
            while (true)
            {
                var seqid = buffer[3];
                buffer[3] = 0;
                var leng = BitConverter.ToInt32(buffer, 0);
                if (leng == 5 && buffer[4] == 0xfe)
                    return result;
                List<string> temp = new List<string>();
                var row = buffer.Skip(4).Take(leng).ToArray();
                for (int i = 0; i < colCount; i++)
                {
                    byte cl = 0;
                    var strlen = getStrLeng(row, ref cl);
                    temp.Add(Encoding.UTF8.GetString(row, cl, (int)strlen));
                    row = row.Skip((int)strlen + cl).ToArray();
                }
                result.Add(temp);
                buffer = buffer.Skip(leng + 4).ToArray();
            }
        }
        private static long getStrLeng(byte[] buffer, ref byte lencount)
        {
            if (buffer[0] <= 0xfb)
            {
                lencount = 0x01;
                return buffer[0] == 0xfb ? 0 : buffer[0];
            }
            if (buffer[0] == 0xfc)
            {
                lencount = 0x03;
                return BitConverter.ToInt16(buffer, 1);
            }

            if (buffer[0] == 0xfd)
            {
                lencount = 0x05;
                return BitConverter.ToInt32(buffer, 1);
            }

            if (buffer[0] == 0xfe)
            {
                lencount = 0x09;
                return BitConverter.ToInt64(buffer, 1);
            }
            return 0;
        }
        static void ParseLoginResult(byte[] buffer)
        {
            var seq = buffer[3];
            buffer[3] = 0x00;
            var leng = BitConverter.ToUInt32(buffer, 0);
            buffer = buffer.Skip(4).ToArray();
            if (buffer[0] == 0xfe)
            {
                return;
            }
        }
        static int parseEOF(byte[] buffer)
        {
            var seq = buffer[3];
            buffer[3] = 0;
            var leng = BitConverter.ToInt32(buffer, 0);
            return leng + 4;
        }
        static int parseCount(byte[] buf, ref int packLen)
        {
            var len = buf.Take(4).ToArray();
            byte seq = len[3];
            len[3] = 0x00;
            int packLeng = BitConverter.ToInt32(len, 0);
            packLen = 4 + packLeng;
            return buf[4];
        }
        static bool IsEOFPack(byte[] buffer)
        {
            if (buffer[4] == 0xfe)
                return true;
            return false;
        }
        static List<ColDefine> ParseColDefine(byte[] buffer, ref int packLen)
        {
            List<ColDefine> resultSet = new List<ColDefine>();
            while (!IsEOFPack(buffer))
            {
                //ColDefine result = new ColDefine();
                //var len = buffer.Skip(packLen).Take(4).ToArray();
                //byte seq = len[3];
                //len[3] = 0x00;
                //int packLeng = BitConverter.ToInt32(len, 0);
                //packLen = 4 + packLeng;
                //result.catalog = getLenencStr(buffer, ref start);
                //result.schema = getLenencStr(buffer, ref start);
                //result.table = getLenencStr(buffer, ref start);
                //result.org_table = getLenencStr(buffer, ref start);
                //result.name = getLenencStr(buffer, ref start);
                //result.org_name = getLenencStr(buffer, ref start);
                //start++;
                //result.charset = buffer.Skip(start).Take(2).ToArray();
                //start += 2;
                //result.ColumLength = BitConverter.ToInt32(buffer, start);
                //start += 4;
                //result.type = buffer[start];
                //start++;
                //result.flags = buffer.Skip(start).Take(2).ToArray();
                //start += 2;
                //result.decimals = buffer[start];
                //start++;
                //start += 2;
                ColDefine result = null;
                int packLength = ColDefine.ParseColDefinePack(buffer, ref result);
                if (packLength == 0)
                    throw new Exception();
                buffer = buffer.Skip(packLength).ToArray();
                packLen += packLength;
                resultSet.Add(result);
            }
            return resultSet;
        }
    }

    public class FORMAT_DESCRIPTION_EVENT
    {
        public int version;
        public string serverVersion;
        public DateTime timestamp;
        public int eventHeadLength;
        public byte[] eventTypeHeaderLength;
        public static FORMAT_DESCRIPTION_EVENT Create(byte[] data)
        {
            var result = new FORMAT_DESCRIPTION_EVENT();
            result.version = BitConverter.ToUInt16(data, 0);
            result.serverVersion = Encoding.ASCII.GetString(data, 2, 50);
            result.timestamp = new DateTime(1970, 1, 1).AddSeconds(BitConverter.ToInt32(data, 52));
            result.eventHeadLength = data[56];
            //bitmap里面把eventHeadLength当作unknow来使用
            result.eventTypeHeaderLength = data.Skip(56).ToArray();
            return result;
        }
    }

    public enum ParseState
    {
        Inited,
        ParseSetHeaderSuccess,
        ParseFieldsSuccess,
        ParseRowDataSuccess
    }

    public class ResultSet
    {
        private ParseState ParseState;
        private List<byte> Data = new List<byte>();
        private int seq = 0;

        private List<ColDefine> fieldList = new List<ColDefine>();
        private List<List<string>> dataRow = new List<List<string>>();
        private int FieldCount = 0;
        public ResultSet()
        {
            ParseState = ParseState.Inited;
        }

        public string GetData(int row, int col)
        {
            return dataRow[row][col];
        }

        //喂入数据包(解析完整的,去掉length的)
        public void AddData(byte seq, byte[] package)
        {
            this.seq++;
            if (this.seq != seq)
                throw new Exception("数据包乱序");
            if (this.ParseState == ParseState.Inited)
            {
                if (package.Length == 1)
                {
                    this.FieldCount = package[0];
                    this.ParseState = ParseState.ParseSetHeaderSuccess;
                    return;
                }
            }
            if (this.ParseState == ParseState.ParseSetHeaderSuccess)
            {
                if (IsEndOfFile(package))
                {
                    this.ParseState = ParseState.ParseFieldsSuccess;
                    return;
                }
                ColDefine field = ColDefine.Parse(package);
                if(field == null)
                    throw new Exception("field parse error");
                fieldList.Add(field);
                return;
            }
            if (this.ParseState == ParseState.ParseFieldsSuccess)
            {
                if (IsEndOfFile(package))
                {
                    this.ParseState = ParseState.ParseRowDataSuccess;
                    return;
                }
                List<string> list = new List<string>();
                while (package.Length > 0)
                {
                    byte lenleng = 0;
                    var resu = getStrLeng(package, ref lenleng);
                    var data = Encoding.ASCII.GetString(package, lenleng, (int)resu);
                    list.Add(data);
                    package = package.Skip(lenleng + (int)resu).ToArray();
                }
                this.dataRow.Add(list);
                return;
            }
            if (this.ParseState == ParseState.ParseRowDataSuccess)
            {
                throw new Exception("data is parsed!");
            }
        }

        private bool IsEndOfFile(byte[] data)
        {
            if (data[0] == 0xfe)
                return true;
            return false;
        }
        private static long getStrLeng(byte[] buffer, ref byte lencount)
        {
            if (buffer[0] <= 0xfb)
            {
                lencount = 0x01;
                return buffer[0] == 0xfb ? 0 : buffer[0];
            }
            if (buffer[0] == 0xfc)
            {
                lencount = 0x03;
                return BitConverter.ToInt16(buffer, 1);
            }

            if (buffer[0] == 0xfd)
            {
                lencount = 0x05;
                return BitConverter.ToInt32(buffer, 1);
            }

            if (buffer[0] == 0xfe)
            {
                lencount = 0x09;
                return BitConverter.ToInt64(buffer, 1);
            }
            return 0;
        }
    }

    public class Response
    {
        //4byte
        public byte[] Capability_flags;
        public int Max_Packet_Size;
        public byte CharSet;
        public byte[] reserved = new byte[23];
        public string UName = null;
        public string authResp = null;
        public string DBName = null;
        public byte[] auth_resp = null;
        public string dbName;
        public Response(string uname, string pwd, byte[] auth_plugin_data, string dbName)
        {
            this.Capability_flags = new byte[] { 0x0f, 0x82, 0x00, 0x00 };
            this.Max_Packet_Size = 50000;
            this.UName = uname;
            var provide = new SHA1CryptoServiceProvider();
            var byte1 = provide.ComputeHash(Encoding.ASCII.GetBytes(pwd));
            var ls = new List<byte>();
            ls.AddRange(auth_plugin_data);
            ls.AddRange(provide.ComputeHash(byte1));
            var byte2 = provide.ComputeHash(ls.ToArray());
            for (int i = 0; i < 20; i++)
                byte1[i] = (byte)(byte1[i] ^ byte2[i]);
            auth_resp = byte1;
            this.dbName = dbName;
        }
        public byte[] toArray()
        {
            var temp = new List<byte>();
            temp.AddRange(this.Capability_flags);
            temp.AddRange(BitConverter.GetBytes(this.Max_Packet_Size));
            temp.Add(this.CharSet);
            temp.AddRange(this.reserved);
            temp.AddRange(Encoding.ASCII.GetBytes(this.UName));
            temp.Add(0x00);
            temp.Add((byte)auth_resp.Length);
            temp.AddRange(auth_resp);
            temp.AddRange(Encoding.ASCII.GetBytes(this.dbName));
            temp.Add(0x00);
            var allCount = temp.Count;
            temp.InsertRange(0, BitConverter.GetBytes(allCount));
            temp[3] = 0x01;
            return temp.ToArray();
        }
    }
    public class ColDefine
    {
        private byte seq;
        public string catalog;
        public string schema;
        public string table;
        public string org_table;
        public string name;
        public string org_name;
        public byte loffixlength;//alway 0c
        public byte[] charset;
        public int ColumLength;
        public byte type;
        public byte[] flags;
        public byte decimals;
        public byte[] filler; //alway 00 00
                              //if command was com_field_list (加入两个字段)

        public static ColDefine Parse(byte[] buffer)
        {
            ColDefine result = new ColDefine();
            int index = 0;
            result.catalog = getLenencStr(buffer, ref index);
            result.schema = getLenencStr(buffer, ref index);
            result.table = getLenencStr(buffer, ref index);
            result.org_table = getLenencStr(buffer, ref index);
            result.name = getLenencStr(buffer, ref index);
            result.org_name = getLenencStr(buffer, ref index);
            index++;
            result.charset = buffer.Skip(index).Take(2).ToArray();
            index += 2;
            result.ColumLength = BitConverter.ToInt32(buffer, index);
            index += 4;
            result.type = buffer[index];
            index++;
            result.flags = buffer.Skip(index).Take(2).ToArray();
            index += 2;
            result.decimals = buffer[index];
            index++;
            return result;
        }

        public static int ParseColDefinePack(byte[] buffer, ref ColDefine data)
        {
            var result = new ColDefine();
            result.seq = buffer[3];
            buffer[3] = 0x00;
            var packLeng = BitConverter.ToInt32(buffer, 0);
            int index = 4;
            result.catalog = getLenencStr(buffer, ref index);
            result.schema = getLenencStr(buffer, ref index);
            result.table = getLenencStr(buffer, ref index);
            result.org_table = getLenencStr(buffer, ref index);
            result.name = getLenencStr(buffer, ref index);
            result.org_name = getLenencStr(buffer, ref index);
            index++;
            result.charset = buffer.Skip(index).Take(2).ToArray();
            index += 2;
            result.ColumLength = BitConverter.ToInt32(buffer, index);
            index += 4;
            result.type = buffer[index];
            index++;
            result.flags = buffer.Skip(index).Take(2).ToArray();
            index += 2;
            result.decimals = buffer[index];
            index++;
            index += 2;
            data = result;
            return packLeng + 4;
        }
        private static string getLenencStr(byte[] buffer, ref int index)
        {
            string result = null;
            if (buffer[index] != 0x00)
            {
                result = Encoding.ASCII.GetString(buffer, index + 1, buffer[index]);
                index += buffer[index];
            }
            index++;
            return result;
        }
    }
    public class ServerReq
    {
        private byte[] buffer;
        public ServerReq(byte[] buffer)
        {
            this.buffer = buffer;
        }
        public byte Version;
        public string ServerInfo;
        byte[] ConnectionID;
        byte[] auth_plugin_data_part1;
        byte[] ServerPropertyLow;
        byte CharSet;
        byte[] ServerState;
        byte[] ServerPropertyHeight;
        byte auth_plugin_dataLength = 0;
        public byte[] auth_plugin_data_part2;
        public string auth_plugin_name;
        public int CapabilityFlag
        {
            get
            {
                if (_capabilityFlag == null)
                {
                    _capabilityFlag = new byte[4];
                    _capabilityFlag[0] = this.ServerPropertyLow[0];
                    _capabilityFlag[1] = this.ServerPropertyLow[1];
                    _capabilityFlag[2] = this.ServerPropertyHeight[0];
                    _capabilityFlag[3] = this.ServerPropertyHeight[1];
                }
                return BitConverter.ToInt32(_capabilityFlag, 0);
            }
        }
        private byte[] _capabilityFlag;
        const int CLIENT_PLUGIN_AUTH = 0x00080000;
        const int CLIENT_SECURE_CONNECTION = 0x00008000;
        public byte[] AuthData
        {
            get
            {
                var temp = new List<byte>();
                temp.AddRange(this.auth_plugin_data_part1);
                temp.AddRange(this.auth_plugin_data_part2);
                return temp.ToArray();
            }
        }
        public void StartParse()
        {
            if (buffer.Length < 4)
                throw new Exception();
            byte seqNum = buffer[3];
            buffer[3] = 0x00;
            var length = BitConverter.ToInt32(buffer, 0);
            if ((length + 4) != buffer.Length)
                throw new Exception();
            Version = buffer[4];
            int i = 5;
            for (; i < buffer.Length; i++)
            {
                if (buffer[i] == 0x00)
                    break;
            }
            ServerInfo = Encoding.ASCII.GetString(buffer, 5, i - 5);
            i++;
            this.ConnectionID = buffer.Skip(i).Take(4).ToArray();
            i += 4;
            this.auth_plugin_data_part1 = buffer.Skip(i).Take(8).ToArray();
            i += 9;
            this.ServerPropertyLow = buffer.Skip(i).Take(2).ToArray();
            i += 2;
            this.CharSet = buffer[i];
            i++;
            this.ServerState = buffer.Skip(i).Take(2).ToArray();
            i += 2;
            this.ServerPropertyHeight = buffer.Skip(i).Take(2).ToArray();
            i += 2;
            if ((this.CapabilityFlag & CLIENT_PLUGIN_AUTH) != 0)
            {
                this.auth_plugin_dataLength = buffer[i];
            }
            //reserved and auth_plugin_dataLength
            i += 11;
            if ((this.CapabilityFlag & CLIENT_SECURE_CONNECTION) != 0)
            {
                int temp = Math.Max(13, this.auth_plugin_dataLength - 8);
                this.auth_plugin_data_part2 = buffer.Skip(i).Take(temp).ToArray();
                i += temp;
            }
            if ((this.CapabilityFlag & CLIENT_PLUGIN_AUTH) != 0)
            {
                int start = i;
                for (; i < buffer.Length; i++)
                {
                    if (i == 0)
                        break;
                }
                this.auth_plugin_name = Encoding.ASCII.GetString(buffer, start, i - start - 1);
            }
        }
    }
}
