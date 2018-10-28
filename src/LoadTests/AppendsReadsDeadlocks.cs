namespace SqlStreamStore
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using System.Linq;
    using System.Threading;
    using EasyConsole;
    using LoadTests;
    using Shouldly;
    using SqlStreamStore.Logging;
    using SqlStreamStore.Streams;
    using Common.Async;
    using Xunit;

    public partial class AppendsReadsDeadlocks : LoadTest
    {
        [Fact]
        public async Task Concurrent_appends_and_reads_do_not_cause_deadlocks()
        {
            CancellationToken ct =new CancellationToken();
            await RunAsync(ct);
        }

        protected override async Task RunAsync(CancellationToken ct)
        {
            int chunksCount = 1000;
            int chunkSize = 100;
            using(var fixture = GetFixture())
            {
                using(var store = await fixture.GetStreamStore())
                {
                    var stopwatch = Stopwatch.StartNew();
                    Task saveTask = GenerateMessages(store, chunksCount, chunkSize);
                    Task readTask = GetManyPagesAsync(store, chunksCount, chunkSize);

                    await Task.WhenAll(saveTask, readTask);
                    // stopwatch ;
                }
            }
        }

        private StreamStoreAcceptanceTestFixture GetFixture()
        {
            var fixture = new MsSqlStreamStoreFixture("dbo"); //new MsSqlStreamStoreV3Fixture("dbo");
            Console.WriteLine(fixture.ConnectionString);
          //  streamStore = fixture.GetStreamStore().Result;
            return fixture;
        }

        private static async Task GenerateMessages( IStreamStore store, int chunksCount, int chunkSize)
        {
            var dictStreamMessage = new Dictionary<string, NewStreamMessage>();
            var random = new Random();
            var messageJsonDataSize = 50 * 1024;
            for (int n = 0; n < chunksCount; n++)
            {
                var stopwatch = Stopwatch.StartNew();
                for (int i = 0; i < chunkSize; i++) //generate chunk of messages
                {
                    string jsonData = $"message-{n * chunksCount + i}" + new string('m', random.Next( messageJsonDataSize)  );
                    var message = new NewStreamMessage(Guid.NewGuid(), jsonData, "{}", $"{i}");
                    var streamId = $"streamId{random.Next(n * chunksCount + i)}";
                    dictStreamMessage[streamId] = message;
                }
                //await -in-parallel
                await dictStreamMessage.ForEachAsync(chunkSize,
                    async kvp => { await store.AppendToStream(kvp.Key, ExpectedVersion.Any, kvp.Value); },
                    t =>
                    {
                        //will be called only if t.IsFaulted  
                        var exception = t.Exception;
                        var errorMessage = $"Task {t.Id} failed " + exception?.GetBaseException().Message;
                        throw new Exception(errorMessage, exception);
                    });
                Console.WriteLine($"Chunk number {n} saved. Elapsed {stopwatch.Elapsed} ");
            }
        }

        public async Task<long> GetManyPagesAsync(IStreamStore store, int chunksCount, int batchSize)
        {
            ReadAllPage page;
            long start = 0;
            //var events = new List<StreamMessage>();
            for (int i = 0; i < chunksCount; i++)
            {
                var stopwatch = Stopwatch.StartNew();
                var timesSlept = 0;
                bool moreThanPage = false;
                while (!moreThanPage)
                {
                    page = await store.ReadAllForwards(start, batchSize);
                    if (page.IsEnd)
                    {

                        Thread.Sleep(10);
                        timesSlept++;
                    }
                    else
                    {
                        start = page.NextPosition;
                        moreThanPage = true;
                        Console.WriteLine($"Page start from {start} read. Slept {timesSlept} times by 10ms.Elapsed {stopwatch.Elapsed}");
                    }
                }
            }

            return start;
        }
    }
}


