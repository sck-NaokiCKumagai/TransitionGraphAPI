using Microsoft.Extensions.Caching.Memory;
using System;

namespace GlobalCache
{
    /// <summary>
    /// Cache-Cacheに関する処理
    /// 処理概略：Cacheに関する処理をまとめる
    /// </summary>
    public static class Cache
    {
        private static readonly MemoryCache memoryCache = new(new MemoryCacheOptions());
        private static readonly MemoryCache _cache = memoryCache;

        /// <summary>
        /// cacheへのデータセット
        /// </summary>
        /// <param name="key">キー</param>
        /// <param name="value">セットデータ</param>        
        public static void Set(string key, object value)
        {
            // 今日中は作ったcacheデータは保持する(翌日には消す)
            DateTime endOfToday = DateTime.Today.AddDays(1).AddSeconds(-1);

            var options = new MemoryCacheEntryOptions()
                .SetAbsoluteExpiration(endOfToday)
                // キャッシュ削除時に実行されるコールバックを登録
                .RegisterPostEvictionCallback(EvictionLogCallback);

            _cache.Set(key, value, options);
        }

        /// <summary>
        /// cacheデータの削除時のログ出力
        /// </summary>
        /// <param name="key">キー</param>
        /// <param name="value">データ(不使用)</param>
        /// <param name="reason">理由</param>
        /// <param name="state">ステータス(不使用)</param>
        private static void EvictionLogCallback(object key, object? value, EvictionReason reason, object? state)
        {
            // ここでログ出力処理を行う
            string message = $"[Cache Log] Key: {key}, Reason: {reason}, Time: {DateTime.Now}";

            // 実際の実装では、ここでLogファイルへの書き込みなどを行う
            Console.WriteLine(message);
            // System.Diagnostics.Debug.WriteLine(message);
        }

        /// <summary>
        /// cacheデータの取出し
        /// </summary>
        /// <typeparam name="T">戻り値型</typeparam>
        /// <param name="key">キー</param>
        /// <returns>cacheデータ</returns>
        public static T? Get<T>(string key)
            => _cache.TryGetValue(key, out T? value) ? value : default;

        /// <summary>
        /// cacheの削除
        /// </summary>
        /// <param name="key">キー</param>
        public static void Remove(string key)
            => _cache.Remove(key);
    }
}