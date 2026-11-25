$ErrorActionPreference = "Stop"

$serverSource = @'
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

public static class NeonOutpostServer
{
    private const int Port = 3000;
    private static readonly object RoomsLock = new object();
    private static readonly Dictionary<string, List<PlayerConnection>> Rooms =
        new Dictionary<string, List<PlayerConnection>>(StringComparer.OrdinalIgnoreCase);
    private static string GameFile;
    private static TcpListener Listener;

    private sealed class PlayerConnection
    {
        public TcpClient Client;
        public NetworkStream Stream;
        public string Room;
        public string PlayerId;
        public readonly object SendLock = new object();
    }

    public static void Run(string gameFile)
    {
        GameFile = gameFile;
        Listener = new TcpListener(IPAddress.Any, Port);
        Listener.Start();

        Console.WriteLine("Neon Outpost LAN server is running.");
        Console.WriteLine("On this laptop: http://localhost:{0}/", Port);
        foreach (IPAddress address in Dns.GetHostAddresses(Dns.GetHostName())
            .Where(item => item.AddressFamily == AddressFamily.InterNetwork && !IPAddress.IsLoopback(item))
            .Distinct())
        {
            Console.WriteLine("On the same Wi-Fi: http://{0}:{1}/", address, Port);
        }
        Console.WriteLine("Keep this window open while you play. Press Ctrl+C to stop.");

        while (true)
        {
            TcpClient client = Listener.AcceptTcpClient();
            Thread worker = new Thread(delegate() { HandleClient(client); });
            worker.IsBackground = true;
            worker.Start();
        }
    }

    private static void HandleClient(TcpClient client)
    {
        PlayerConnection peer = null;
        try
        {
            client.NoDelay = true;
            NetworkStream stream = client.GetStream();
            string request = ReadHttpHeaders(stream);
            if (request == null) return;

            string[] lines = request.Split(new[] { "\r\n" }, StringSplitOptions.None);
            string[] requestParts = lines[0].Split(' ');
            if (requestParts.Length < 2 || requestParts[0] != "GET")
            {
                WriteHttpError(stream, 405, "Method Not Allowed");
                return;
            }

            string path = requestParts[1].Split('?')[0];
            string upgrade = GetHeader(lines, "Upgrade");
            if (path == "/multiplayer" && string.Equals(upgrade, "websocket", StringComparison.OrdinalIgnoreCase))
            {
                string key = GetHeader(lines, "Sec-WebSocket-Key");
                if (String.IsNullOrEmpty(key))
                {
                    WriteHttpError(stream, 400, "Bad Request");
                    return;
                }

                AcceptWebSocket(stream, key);
                peer = new PlayerConnection { Client = client, Stream = stream };
                HandleWebSocket(peer);
                return;
            }

            if (path != "/" && path != "/arena.html")
            {
                WriteHttpError(stream, 404, "Not Found");
                return;
            }

            byte[] content = File.ReadAllBytes(GameFile);
            string response = "HTTP/1.1 200 OK\r\n" +
                "Content-Type: text/html; charset=utf-8\r\n" +
                "Cache-Control: no-store\r\n" +
                "Connection: close\r\n" +
                "Content-Length: " + content.Length + "\r\n\r\n";
            byte[] header = Encoding.ASCII.GetBytes(response);
            stream.Write(header, 0, header.Length);
            stream.Write(content, 0, content.Length);
        }
        catch (IOException) { }
        catch (SocketException) { }
        catch (ObjectDisposedException) { }
        finally
        {
            if (peer != null) RemovePlayer(peer);
            client.Close();
        }
    }

    private static string ReadHttpHeaders(NetworkStream stream)
    {
        using (MemoryStream buffer = new MemoryStream())
        {
            int matched = 0;
            while (buffer.Length < 16384)
            {
                int value = stream.ReadByte();
                if (value < 0) return null;
                buffer.WriteByte((byte)value);
                if ((matched == 0 && value == '\r') ||
                    (matched == 1 && value == '\n') ||
                    (matched == 2 && value == '\r') ||
                    (matched == 3 && value == '\n'))
                {
                    matched++;
                    if (matched == 4) return Encoding.ASCII.GetString(buffer.ToArray());
                }
                else
                {
                    matched = value == '\r' ? 1 : 0;
                }
            }
            return null;
        }
    }

    private static string GetHeader(string[] lines, string name)
    {
        string prefix = name + ":";
        foreach (string line in lines)
        {
            if (line.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                return line.Substring(prefix.Length).Trim();
        }
        return null;
    }

    private static void WriteHttpError(NetworkStream stream, int status, string reason)
    {
        byte[] response = Encoding.ASCII.GetBytes(
            "HTTP/1.1 " + status + " " + reason + "\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
        stream.Write(response, 0, response.Length);
    }

    private static void AcceptWebSocket(NetworkStream stream, string key)
    {
        string combined = key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
        byte[] digest;
        using (SHA1 sha1 = SHA1.Create())
            digest = sha1.ComputeHash(Encoding.ASCII.GetBytes(combined));

        string response = "HTTP/1.1 101 Switching Protocols\r\n" +
            "Upgrade: websocket\r\n" +
            "Connection: Upgrade\r\n" +
            "Sec-WebSocket-Accept: " + Convert.ToBase64String(digest) + "\r\n\r\n";
        byte[] bytes = Encoding.ASCII.GetBytes(response);
        stream.Write(bytes, 0, bytes.Length);
        stream.Flush();
    }

    private static byte[] ReadExact(NetworkStream stream, int count)
    {
        byte[] result = new byte[count];
        int offset = 0;
        while (offset < count)
        {
            int read = stream.Read(result, offset, count - offset);
            if (read == 0) return null;
            offset += read;
        }
        return result;
    }

    private static void HandleWebSocket(PlayerConnection peer)
    {
        while (peer.Client.Connected)
        {
            byte[] header = ReadExact(peer.Stream, 2);
            if (header == null) return;
            int opcode = header[0] & 0x0F;
            bool masked = (header[1] & 0x80) != 0;
            ulong length = (ulong)(header[1] & 0x7F);

            if (length == 126)
            {
                byte[] extended = ReadExact(peer.Stream, 2);
                if (extended == null) return;
                length = (ulong)((extended[0] << 8) | extended[1]);
            }
            else if (length == 127)
            {
                byte[] extended = ReadExact(peer.Stream, 8);
                if (extended == null || extended.Take(4).Any(value => value != 0)) return;
                length = ((ulong)extended[4] << 24) | ((ulong)extended[5] << 16) |
                    ((ulong)extended[6] << 8) | extended[7];
            }

            if (length > 16384) return;
            byte[] mask = masked ? ReadExact(peer.Stream, 4) : null;
            byte[] payload = ReadExact(peer.Stream, (int)length);
            if (payload == null || (masked && mask == null)) return;
            if (masked)
                for (int i = 0; i < payload.Length; i++) payload[i] = (byte)(payload[i] ^ mask[i % 4]);

            if (opcode == 8) return;
            if (opcode == 9)
            {
                SendFrame(peer, 10, payload);
                continue;
            }
            if (opcode != 1) continue;

            string message = Encoding.UTF8.GetString(payload);
            HandleMessage(peer, message);
        }
    }

    private static string GetJsonString(string json, string name)
    {
        Match match = Regex.Match(json,
            "\"" + Regex.Escape(name) + "\"\\s*:\\s*\"([a-zA-Z0-9_-]*)\"");
        return match.Success ? match.Groups[1].Value : null;
    }

    private static void HandleMessage(PlayerConnection peer, string json)
    {
        string type = GetJsonString(json, "type");
        if (type == "join")
        {
            string room = GetJsonString(json, "room");
            string id = GetJsonString(json, "id");
            if (String.IsNullOrEmpty(room) || room.Length > 24 ||
                !Regex.IsMatch(room, "^[a-zA-Z0-9_-]+$") || String.IsNullOrEmpty(id))
            {
                SendText(peer, "{\"type\":\"error\",\"message\":\"Invalid room code\"}");
                return;
            }

            peer.Room = room.ToLowerInvariant();
            peer.PlayerId = id;
            List<PlayerConnection> existing;
            lock (RoomsLock)
            {
                if (!Rooms.TryGetValue(peer.Room, out existing))
                {
                    existing = new List<PlayerConnection>();
                    Rooms.Add(peer.Room, existing);
                }
                existing = existing.ToList();
                Rooms[peer.Room].Add(peer);
            }
            SendText(peer, "{\"type\":\"joined\",\"room\":\"" + peer.Room + "\"}");
            foreach (PlayerConnection member in existing)
                SendText(member, "{\"type\":\"hello\",\"id\":\"" + id + "\"}");
            Console.WriteLine("Player joined room " + room.ToUpperInvariant());
            return;
        }

        if (peer.Room == null || (type != "state" && type != "shot" && type != "chat" && type != "bye"))
            return;
        Broadcast(peer, json);
    }

    private static void Broadcast(PlayerConnection sender, string message)
    {
        List<PlayerConnection> members;
        lock (RoomsLock)
        {
            if (sender.Room == null || !Rooms.TryGetValue(sender.Room, out members)) return;
            members = members.ToList();
        }
        foreach (PlayerConnection member in members)
            if (!Object.ReferenceEquals(member, sender)) SendText(member, message);
    }

    private static void SendText(PlayerConnection peer, string text)
    {
        SendFrame(peer, 1, Encoding.UTF8.GetBytes(text));
    }

    private static void SendFrame(PlayerConnection peer, int opcode, byte[] payload)
    {
        if (payload.Length > 65535) return;
        byte[] frame;
        if (payload.Length < 126)
        {
            frame = new byte[payload.Length + 2];
            frame[1] = (byte)payload.Length;
        }
        else
        {
            frame = new byte[payload.Length + 4];
            frame[1] = 126;
            frame[2] = (byte)(payload.Length >> 8);
            frame[3] = (byte)payload.Length;
        }
        frame[0] = (byte)(0x80 | opcode);
        Buffer.BlockCopy(payload, 0, frame, frame.Length - payload.Length, payload.Length);
        try
        {
            lock (peer.SendLock) peer.Stream.Write(frame, 0, frame.Length);
        }
        catch (IOException) { }
        catch (ObjectDisposedException) { }
    }

    private static void RemovePlayer(PlayerConnection peer)
    {
        if (peer.Room == null) return;
        lock (RoomsLock)
        {
            List<PlayerConnection> members;
            if (Rooms.TryGetValue(peer.Room, out members))
            {
                members.Remove(peer);
                if (members.Count == 0) Rooms.Remove(peer.Room);
            }
        }
        Broadcast(peer, "{\"type\":\"bye\",\"id\":\"" + peer.PlayerId + "\"}");
    }
}
'@

Add-Type -TypeDefinition $serverSource -Language CSharp -ErrorAction Stop
$gameFile = Join-Path $PSScriptRoot "arena.html"
[NeonOutpostServer]::Run($gameFile)
