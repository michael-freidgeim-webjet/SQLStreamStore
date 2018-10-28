namespace LoadTests
{
    using System;
    using System.Diagnostics;
    using System.Threading;
    using EasyConsole;
    using Serilog;
    using SqlStreamStore;

    internal class Program
    {
        static void Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo
                .File("LoadTests.txt")
                .CreateLogger();

            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, __) => cts.Cancel();
            new AppendsReadsDeadlocks().Run(cts.Token);;
            Output.WriteLine(ConsoleColor.Yellow, "Choose a test:");
            new Menu()
                .Add(
                    "Append with ExpectedVersion.Any",
                    () => new AppendExpectedVersionAnyParallel().Run(cts.Token))
                .Add(
                    "Read all",
                    () => new ReadAll().Run(cts.Token))
                .Add(
                    "Append max count",
                    () => new AppendMaxCount().Run(cts.Token))
                .Add(
                    "Many steam subscriptions",
                    () => new StreamSubscription().Run(cts.Token))
                .Add(
                    "Test gaps",
                    () => new TestGaps().Run(cts.Token))
                .Display();

            if(Debugger.IsAttached)
            {
                Console.ReadLine();
            }
        }
    }
}