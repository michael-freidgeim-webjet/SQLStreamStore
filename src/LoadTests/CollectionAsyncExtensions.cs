using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Common.Async
{
    public static class CollectionAsyncExtensions
    {
        //Consider to use AsyncEnumerable instead

        /// <summary>
        /// https://stackoverflow.com/questions/11564506/nesting-await-in-parallel-foreach/25877042#25877042
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source">Should have more records than degreeOfParallelism</param>
        /// <param name="degreeOfParallelism"></param>
        /// <param name="body"></param>
        /// <param name="handleException"></param>
        /// <returns></returns>
        public static async Task ForEachAsync<T>(
            this ICollection<T> source, int degreeOfParallelism, Func<T, Task> body, Action<Task> handleException=null)
        {
            if (source.Count > 0)
            {
                await Task.WhenAll(
                    from partition in Partitioner.Create(source).GetPartitions(degreeOfParallelism)
                    select Task.Run(async delegate
                    {
                        using (partition)
                            while (partition.MoveNext())
                                await body(partition.Current).ContinueWith(t =>
                                {
                                    //observe exceptions
                                    if (t.IsFaulted)
                                    {
                                        handleException?.Invoke(t);
                                    }
                                });
                    }));
            }
        }
    }
}
