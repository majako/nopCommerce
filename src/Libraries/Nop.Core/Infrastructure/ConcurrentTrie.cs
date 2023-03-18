using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Nop.Core.Infrastructure
{
    /// <summary>
    /// A thread-safe implementation of a radix tree
    /// </summary>
    public class ConcurrentTrie<TValue>
    {
        private class TrieNode
        {
            public volatile string Label;
            public readonly Dictionary<char, TrieNode> Children = new();
            public readonly ReaderWriterLockSlim Lock = new();

            public TrieNode(string label)
            {
                Label = label;
            }
        }

        private static readonly ConcurrentDictionary<TrieNode, TValue> _values = new();
        private readonly TrieNode _root;
        private readonly string _prefix;


        /// <summary>
        /// Gets a collection that contains the keys in the <see cref="ConcurrentTrie{TValue}" />
        /// </summary>
        public IEnumerable<string> Keys => Search(string.Empty).Select(kv => kv.Key);

        /// <summary>
        /// Gets a collection that contains the values in the <see cref="ConcurrentTrie{TValue}" />
        /// </summary>
        public IEnumerable<TValue> Values => Search(string.Empty).Select(kv => kv.Value);


        public ConcurrentTrie() : this(new(string.Empty), string.Empty)
        {
        }

        private ConcurrentTrie(TrieNode root, string prefix)
        {
            _root = root;
            _prefix = prefix;
        }

        /// <summary>
        /// Attempts to get the value associated with the specified key
        /// </summary>
        /// <param name="key">The key of the item to get (case-insensitive)</param>
        /// <param name="value">The value associated with <paramref name="key"/>, if found</param>
        /// <returns>
        /// True if the key was found, otherwise false
        /// </returns>
        public bool TryGetValue(string key, out TValue value)
        {
            if (key is null)
                throw new ArgumentNullException(nameof(key));

            value = default;
            return Find(key, out var node) && _values.TryGetValue(node, out value);
        }

        /// <summary>
        /// Adds a key-value pair to the trie
        /// </summary>
        /// <param name="key">The key of the new item (case-insensitive)</param>
        /// <param name="value">The value to be associated with <paramref name="key"/></param>
        public void Add(string key, TValue value)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentException($"'{nameof(key)}' cannot be null or empty.", nameof(key));

            var node = GetOrAddNode(key);
            _values[node] = value;
        }

        /// <summary>
        /// Clears the trie
        /// </summary>
        public void Clear()
        {
            _root.Lock.EnterWriteLock();
            _root.Children.Clear();
            _root.Lock.ExitWriteLock();
        }

        /// <summary>
        /// Gets all key-value pairs for keys starting with the given prefix
        /// </summary>
        /// <param name="prefix">The prefix to search for (case-insensitive)</param>
        /// <returns>
        /// All key-value pairs for keys starting with <paramref name="prefix"/>
        /// </returns>
        public IEnumerable<KeyValuePair<string, TValue>> Search(string prefix)
        {
            if (prefix is null)
                throw new ArgumentNullException(nameof(prefix));

            if (!Find(prefix, out var node))
                return Enumerable.Empty<KeyValuePair<string, TValue>>();

            // depth-first traversal
            IEnumerable<KeyValuePair<string, TValue>> traverse(TrieNode n, string s)
            {
                n.Lock.EnterReadLock();
                if (_values.TryGetValue(n, out var value))
                    yield return new KeyValuePair<string, TValue>(_prefix + s, value);
                foreach (var child in n.Children.Values)
                {
                    foreach (var kv in traverse(child, s + child.Label))
                        yield return kv;
                }
                n.Lock.ExitReadLock();
            }
            return traverse(node, prefix);
        }

        /// <summary>
        /// Removes the item with the given key, if present
        /// </summary>
        /// <param name="key">The key of the item to be removed (case-insensitive)</param>
        public void Remove(string key)
        {
            Remove(_root, key);
        }

        /// <summary>
        /// Gets the value with the specified key, adding a new item if one does not exist
        /// </summary>
        /// <param name="key">The key of the item to be deleted (case-insensitive)</param>
        /// <param name="valueFactory">A function for producing a new value if one was not found</param>
        /// <returns>
        /// The existing value for the given key, if found, otherwise the newly inserted value
        /// </returns>
        public TValue GetOrAdd(string key, Func<TValue> valueFactory)
        {
            var node = GetOrAddNode(key);
            return _values.GetOrAdd(node, _ => valueFactory());
        }

        /// <summary>
        /// Attempts to remove all items with keys starting with the specified prefix
        /// </summary>
        /// <param name="prefix">The prefix of the items to be deleted (case-insensitive)</param>
        /// <param name="subtree">The subtree containing all deleted items, if found</param>
        /// <returns>
        /// True if the prefix was successfully removed from the trie, otherwise false
        /// </returns>
        public bool Prune(string prefix, out ConcurrentTrie<TValue> subtree)
        {
            if (string.IsNullOrEmpty(prefix))
                throw new ArgumentException($"'{nameof(prefix)}' cannot be null or empty.", nameof(prefix));

            subtree = default;
            if (!prefix.StartsWith(_prefix))
                return false;
            if (prefix == _prefix)
            {
                var rootCopy = new TrieNode(string.Empty);
                _root.Lock.EnterReadLock();
                try
                {
                    foreach (var (key, child) in _root.Children)
                        rootCopy.Children[key] = child;
                }
                finally
                {
                    _root.Lock.ExitReadLock();
                }
                subtree = new(rootCopy, prefix);
                Clear();
                return true;
            }
            var node = _root;
            var parent = node;
            var span = prefix.AsSpan()[_prefix.Length..];
            var i = 0;
            char c;
            while (i < span.Length)
            {
                c = span[i];
                if (!node.Children.TryGetValue(c, out node) || node == default) // see footnote 1
                    return false;
                var label = node.Label.AsSpan();
                var k = GetCommonPrefixLength(span[i..], label);
                if (k == span.Length - i)
                {
                    parent.Lock.EnterWriteLock();
                    try
                    {
                        if (parent.Children.Remove(c, out node))
                        {
                            subtree = new(node, prefix);
                            return true;
                        }
                    }
                    finally
                    {
                        parent.Lock.ExitWriteLock();
                    }
                    return false;   // was removed by another thread
                }
                if (k < label.Length)
                    return false;
                i += label.Length;
                parent = node;
            }
            return false;
        }

        private TrieNode GetOrAddNode(string key)
        {
            var node = _root;
            var suffix = key.AsSpan();
            char c;
            ReaderWriterLockSlim rw;
            while (true)
            {
                c = suffix[0];
                rw = node.Lock;
                rw.EnterUpgradeableReadLock();
                try
                {
                    if (node.Children.TryGetValue(c, out var nextNode))
                    {
                        var nextKey = nextNode.Label.AsSpan();
                        var i = GetCommonPrefixLength(nextKey, suffix);
                        if (i == nextKey.Length)   // suffix starts with nextKey
                        {
                            if (i == suffix.Length)    // keys are equal
                                return nextNode;
                            suffix = suffix[nextKey.Length..];
                            node = nextNode;
                            continue;
                        }
                        nextNode.Label = nextKey[i..].ToString();
                        var splitNode = new TrieNode(suffix[..i].ToString());
                        splitNode.Children[nextKey[i]] = nextNode;
                        TrieNode outNode;
                        if (i == suffix.Length) // nextKey starts with suffix
                            outNode = splitNode;
                        else
                            splitNode.Children[suffix[i]] = outNode = new TrieNode(suffix[i..].ToString());
                        node.Lock.EnterWriteLock();
                        node.Children[c] = splitNode;
                        node.Lock.ExitWriteLock();
                        return outNode;
                    }
                    var n = new TrieNode(suffix.ToString());
                    node.Lock.EnterWriteLock();
                    node.Children[c] = n;
                    node.Lock.ExitWriteLock();
                    return n;
                }
                finally
                {
                    rw.ExitUpgradeableReadLock();
                }
            }
        }

        private bool Find(string key, out TrieNode node)
        {
            node = _root;
            if (key.Length == 0)
                return true;
            var suffix = key.AsSpan();
            while (true)
            {
                if (!node.Children.TryGetValue(suffix[0], out node) || node == default) // see footnote 1
                    return false;
                var span = node.Label.AsSpan();
                var i = GetCommonPrefixLength(suffix, span);
                if (i == span.Length)
                {
                    if (i == suffix.Length)
                        return _values.TryGetValue(node, out _);
                    suffix = suffix[i..];
                    continue;
                }
                return false;
            }
        }

        private static int GetCommonPrefixLength(ReadOnlySpan<char> s1, ReadOnlySpan<char> s2)
        {
            var i = 0;
            var minLength = Math.Min(s1.Length, s2.Length);
            for (; i < minLength && s2[i] == s1[i]; i++)
                ;
            return i;
        }

        private void Remove(TrieNode node, ReadOnlySpan<char> key)
        {
            if (!key.StartsWith(_prefix))
                return;
            if (key == _prefix)
            {
                _values.TryRemove(_root, out _);
                return;
            }
            var parent = node;
            var span = key[_prefix.Length..];
            var i = 0;
            char c;
            while (i < span.Length)
            {
                c = span[i];
                parent.Lock.EnterUpgradeableReadLock();
                try
                {
                    if (!parent.Children.TryGetValue(c, out node))
                        return;
                    var label = node.Label.AsSpan();
                    var k = GetCommonPrefixLength(span[i..], label);
                    if (k == label.Length && k == span.Length - i)
                    {
                        _values.TryRemove(node, out _);
                        node.Lock.EnterReadLock();
                        try
                        {
                            var nChildren = node.Children.Count;
                            if (nChildren == 0)
                            {
                                parent.Lock.EnterWriteLock();
                                try
                                {
                                    parent.Children.Remove(c, out _);
                                }
                                finally
                                {
                                    parent.Lock.ExitWriteLock();
                                }
                            }
                            else if (nChildren == 1)
                            {
                                var child = node.Children.FirstOrDefault().Value;
                                if (child != default)
                                {
                                    child.Label = node.Label + child.Label;
                                    parent.Lock.EnterWriteLock();
                                    try
                                    {
                                        parent.Children[c] = child;
                                    }
                                    finally
                                    {
                                        parent.Lock.ExitWriteLock();
                                    }
                                }
                            }
                        }
                        finally
                        {
                            node.Lock.ExitReadLock();
                        }
                        return;
                    }
                    if (k < label.Length)
                        return;
                    i += label.Length;
                }
                finally
                {
                    parent.Lock.ExitUpgradeableReadLock();
                }
                parent = node;
            }
        }
    }
}

// Footnotes:
// 
// 1.   Since we optimistically get a value from a non-threadsafe dictionary,
//      it could end up being null despite TryGetValue returning true, so we need to double-check it
//
