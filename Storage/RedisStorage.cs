using StackExchange.Redis;
using NLog;
using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

//how msgpack convert to json?

namespace Storage
{
    public class RedisStorage
    {
        private static ConnectionMultiplexer _conn;
        private const string redisKeyFmt = "scene:group_{0}:type_{1}:id_{2}";
        private readonly int groupID;
        private readonly NLog.Logger _logger = NLog.LogManager.GetCurrentClassLogger();
        public RedisStorage(int groupID)
        {
            this.groupID = groupID;
        }
        public void Connect(string host, int port)
        {
            var cfg = new ConfigurationOptions();
            cfg.EndPoints.Add(host, port);
            _conn = ConnectionMultiplexer.Connect(cfg);
        }
        public async Task<StorageObject> Load(long type, long id)
        {
            //读取全部数据
            RedisKey key = string.Format(redisKeyFmt, groupID, type, id);
            HashEntry[] hashEntries = null;
            try
            {
                hashEntries = await _conn.GetDatabase().HashGetAllAsync(key);
            }
            catch(Exception e)
            {
                _logger.Error(e, $"StorageManager Load({type}, {id}) exception");
            }
            
            Dictionary<string, byte[]> attributes = null;
            if(hashEntries != null)
            {
                attributes = new Dictionary<string, byte[]>(hashEntries.Length);
                foreach(var e in hashEntries)
                {
                    attributes[e.Name.ToString()] = (byte[])e.Value;
                }
            }

            return new StorageObject(type, id, attributes);
        }
        public async Task Save(StorageObject so)
        {
            var entries = so2HashEntries(so);
            if(entries == null) return;
            RedisKey key = string.Format(redisKeyFmt, groupID, so.Type, so.ID);
            await _conn.GetDatabase().HashSetAsync(key, entries);
        }
        private HashEntry[] so2HashEntries(StorageObject so)
        {
            var attributes = so.TypetoString();
            if(attributes == null ||attributes.Count <= 0)
                return null;
          
            //string to hashentry[]
            HashEntry[] hashEntries = new HashEntry[attributes.Count];
            int idx = 0;
            foreach(var kvp in attributes)
            {
                HashEntry e = new HashEntry(kvp.Key, kvp.Value);
                hashEntries[idx++] = e;
            }
            return hashEntries;
        }
        //存attributes
        public async Task Save(long type, long id, Dictionary<string, byte[]> attributes)
        {
            if(attributes == null || attributes.Count <= 0) return;

            RedisKey key = string.Format(redisKeyFmt, groupID, type, id);
            int idx = 0;
            HashEntry[] entries = new HashEntry[attributes.Count];
            foreach(var kvp in attributes)
            {
                entries[idx++] = new HashEntry(kvp.Key, kvp.Value);
            }

            await _conn.GetDatabase().HashSetAsync(key, entries);
        }
    }
}
