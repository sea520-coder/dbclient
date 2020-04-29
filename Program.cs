using System;
using Storage;
using MessagePack;

namespace dbclient
{
    [MessagePackObject(keyAsPropertyName: true)]
    public class FooClass
    {
        public int A;
    }

    class Program
    {
        public async static void test()
        {
            var sto = new RedisStorage(123);
            sto.Connect("127.0.0.1", 6379);

            var data = await sto.Load(1, 2);
            var foo = data.GetOrCreate<FooClass>();
            foo.A = 4;

            await sto.Save(data);
            var data2 = await sto.Load(1, 2);
            var foo2 = data2.GetOrCreate<FooClass>();
            Console.WriteLine($"data err src {foo.A} dst {foo2.A}");
        }
        static void Main(string[] args)
        {
            test();
            Console.ReadLine();
        }
    }
}
