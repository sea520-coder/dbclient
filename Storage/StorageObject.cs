using System.Collections.Generic;
using NLog;
using MessagePack;
using System;

namespace Storage
{
    public class StorageObject
    {
        class StorageValue
        {
            public byte[] Bytes;
            public object Value;
        }
        Dictionary<string, StorageValue> attributes = new Dictionary<string, StorageValue>();
        internal readonly long Type;
        internal readonly long ID;
        internal StorageObject(long t, long id, Dictionary<string, byte[]> rawAttributes = null)
        {
            this.Type = t;
            this.ID = id;
            if(rawAttributes != null)
            {
                foreach(var kvp in rawAttributes)
                {
                    var v = new StorageValue{ Bytes = kvp.Value };
                    attributes[kvp.Key] = v;
                }
            }
        }

        //对外api
        public T GetOrCreate<T>() where T : class, new()
        {
            return GetOrCreate<T>(typeof(T).Name);
        }
        public T GetOrCreate<T>(string key) where T : class, new()
        {
            if(TryGet(key, out T result)) //
            {
                return result;
            }
            result = new T();
            Save(key, result);
            return result;
        }
        
        public bool TryGet<T>(out T v) where T : class, new()
        {
            return TryGet(typeof(T).Name, out v);
        }
        public bool TryGet<T>(string key, out T v) where T : class, new()
        {
            if(attributes.TryGetValue(key, out var sv))
            {
                if(sv.Value == null)
                {
                    try
                    {
                        sv.Value = MessagePackSerializer.Deserialize<T>(sv.Bytes);
                    }
                    catch(Exception e)
                    {
                        NLog.LogManager.GetCurrentClassLogger().Error($"StorageObject Deserialize error {key} {e.ToString()}");
                        v = null;
                        return false;
                    }
                }
                v = (T)sv.Value;
                return true;
            }
            v = null;
            return false;
        }

        private void Save<T>(string key, T v) where T : class, new()
        {
            if(!attributes.TryGetValue(key, out var sv))
            {
                sv = new StorageValue();
                attributes[key] = sv;
            }
            sv.Value = v;
        }
        internal Dictionary<string, byte[]> TypetoString()
        {
            var rawAttributes = new Dictionary<string, byte[]>();

            //序列化attributes数据存盘
            foreach(var kvp in attributes)
            {
                if(kvp.Value.Value == null)
                    continue;

                kvp.Value.Bytes = MessagePackSerializer.Serialize(kvp.Value.Value);
                rawAttributes[kvp.Key] = kvp.Value.Bytes;
            }
            return rawAttributes;
        }
    }
}