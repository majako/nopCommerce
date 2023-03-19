using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Nop.Core.Infrastructure
{
    /// <summary>
    /// A thread-safe implementation of a radix tree
    /// </summary>
    public class ConcurrentTrie<TValue>
    {
        private class TrieNode
        {
            public readonly string Label;
            public readonly Dictionary<char, TrieNode> Children = new();

            public TrieNode(string label)
            {
                Label = label;
            }

            public TrieNode(string label, TrieNode node) : this(label)
            {
                Children = node.Children;
            }
        }

        private record struct InternalSearchResult(string Key, TrieNode Node, TValue Value);

        private static readonly ConcurrentDictionary<TrieNode, TValue> _values = new();
        private volatile TrieNode _root = new(string.Empty);
        private readonly StripedReaderWriterLock _locks = new();

        /// <summary>
        /// Gets a collection that contains the keys in the <see cref="ConcurrentTrie{TValue}" />
        /// </summary>
        public IEnumerable<string> Keys => SearchInternal(_root, string.Empty).Select(t => t.Key);

        /// <summary>
        /// Gets a collection that contains the values in the <see cref="ConcurrentTrie{TValue}" />
        /// </summary>
        public IEnumerable<TValue> Values => SearchInternal(_root, string.Empty).Select(t => t.Value);

        /// <summary>
        /// Initializes a new empty instance of <see cref="ConcurrentTrie{TValue}" />
        /// </summary>
        public ConcurrentTrie()
        {
        }

        private ConcurrentTrie(TrieNode subtreeRoot)
        {
            _root.Children[subtreeRoot.Label[0]] = subtreeRoot;
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
            return Find(key, _root, out var node) && _values.TryGetValue(node, out value);
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

            _values[GetOrAddNode(key)] = value;
        }

        /// <summary>
        /// Clears the trie
        /// </summary>
        public void Clear()
        {
            var oldRoot = _root;
            _root = new(string.Empty);
            // clean up unused values in the background without blocking the thread
            Task.Run(() =>
            {
                foreach (var (_, node, _) in SearchInternal(oldRoot, string.Empty))
                    _values.TryRemove(node, out _);
            });
        }

        /// <summary>
        /// Gets all key-value pairs for keys starting with the given prefix
        /// </summary>
        /// <param name="prefix">The prefix (case-sensitive) to search for</param>
        /// <returns>
        /// All key-value pairs for keys starting with <paramref name="prefix"/>
        /// </returns>
        public IEnumerable<KeyValuePair<string, TValue>> Search(string prefix)
        {
            return SearchInternal(_root, prefix)
                .Select(t => new KeyValuePair<string, TValue>(t.Key, t.Value));
        }

        /// <summary>
        /// Removes the item with the given key, if present
        /// </summary>
        /// <param name="key">The key (case-sensitive) of the item to be removed</param>
        public void Remove(string key)
        {
            Remove(_root, key);
        }

        /// <summary>
        /// Gets the value with the specified key, adding a new item if one does not exist
        /// </summary>
        /// <param name="key">The key (case-sensitive) of the item to be deleted</param>
        /// <param name="valueFactory">A function for producing a new value if one was not found</param>
        /// <returns>
        /// The existing value for the given key, if found, otherwise the newly inserted value
        /// </returns>
        public TValue GetOrAdd(string key, Func<TValue> valueFactory)
        {
            return _values.GetOrAdd(GetOrAddNode(key), _ => valueFactory());
        }

        /// <summary>
        /// Attempts to remove all items with keys starting with the specified prefix
        /// </summary>
        /// <param name="prefix">The prefix (case-sensitive) of the items to be deleted</param>
        /// <param name="subtree">The subtree containing all deleted items, if found</param>
        /// <returns>
        /// True if the prefix was successfully removed from the trie, otherwise false
        /// </returns>
        public bool Prune(string prefix, out ConcurrentTrie<TValue> subtree)
        {
            if (prefix is null)
                throw new ArgumentNullException(nameof(prefix));

            subtree = default;
            var node = _root;
            var parent = node;
            var span = prefix.AsSpan();
            var i = 0;
            while (i < span.Length)
            {
                var c = span[i];
                if (!node.Children.TryGetValue(c, out node) || node == default) // see footnote 1
                    return false;
                var label = node.Label.AsSpan();
                var k = GetCommonPrefixLength(span[i..], label);
                if (k == span.Length - i)
                {
                    var parentLock = _locks.GetLock(parent);
                    parentLock.EnterWriteLock();
                    try
                    {
                        if (parent.Children.Remove(c, out node))
                        {
                            subtree = new(new TrieNode(prefix[..i] + node.Label, node));
                            return true;
                        }
                    }
                    finally
                    {
                        parentLock.ExitWriteLock();
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

        private static int GetCommonPrefixLength(ReadOnlySpan<char> s1, ReadOnlySpan<char> s2)
        {
            var i = 0;
            var minLength = Math.Min(s1.Length, s2.Length);
            for (; i < minLength && s2[i] == s1[i]; i++)
                ;
            return i;
        }

        private static bool Find(string key, TrieNode subtreeRoot, out TrieNode node)
        {
            node = subtreeRoot;
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

        private TrieNode GetOrAddNode(string key)
        {
            var node = _root;
            var suffix = key.AsSpan();
            ReaderWriterLockSlim nodeLock;
            while (true)
            {
                var c = suffix[0];
                nodeLock = _locks.GetLock(node);
                nodeLock.EnterUpgradeableReadLock();
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
                        var splitNode = new TrieNode(suffix[..i].ToString());
                        splitNode.Children[nextKey[i]] = new TrieNode(nextKey[i..].ToString(), nextNode);
                        TrieNode outNode;
                        if (i == suffix.Length) // nextKey starts with suffix
                            outNode = splitNode;
                        else
                            splitNode.Children[suffix[i]] = outNode = new TrieNode(suffix[i..].ToString());
                        nodeLock.EnterWriteLock();
                        try
                        {
                            node.Children[c] = splitNode;
                        }
                        finally
                        {
                            nodeLock.ExitWriteLock();
                        }
                        return outNode;
                    }
                    var suffixNode = new TrieNode(suffix.ToString());
                    nodeLock.EnterWriteLock();
                    try
                    {
                        node.Children[c] = suffixNode;
                    }
                    finally
                    {
                        nodeLock.ExitWriteLock();
                    }
                    return suffixNode;
                }
                finally
                {
                    nodeLock.ExitUpgradeableReadLock();
                }
            }
        }

        private void Remove(TrieNode subtreeRoot, ReadOnlySpan<char> key)
        {
            var node = subtreeRoot;
            var parent = subtreeRoot;
            var i = 0;
            while (i < key.Length)
            {
                var c = key[i];
                var parentLock = _locks.GetLock(parent);
                parentLock.EnterUpgradeableReadLock();
                try
                {
                    if (!parent.Children.TryGetValue(c, out node))
                        return;
                    var label = node.Label.AsSpan();
                    var k = GetCommonPrefixLength(key[i..], label);
                    if (k == label.Length && k == key.Length - i)
                    {
                        _values.TryRemove(node, out _);
                        var nodeLock = _locks.GetLock(node);
                        var lockAlreadyHeld = nodeLock == parentLock;
                        if (!lockAlreadyHeld)
                            nodeLock.EnterReadLock();
                        try
                        {
                            var nChildren = node.Children.Count;
                            if (nChildren == 0)
                            {
                                parentLock.EnterWriteLock();
                                try
                                {
                                    parent.Children.Remove(c, out _);
                                }
                                finally
                                {
                                    parentLock.ExitWriteLock();
                                }
                            }
                            else if (nChildren == 1)
                            {
                                var child = node.Children.FirstOrDefault().Value;
                                if (child != default)
                                {
                                    parentLock.EnterWriteLock();
                                    try
                                    {
                                        parent.Children[c] = new TrieNode(node.Label + child.Label, child);
                                    }
                                    finally
                                    {
                                        parentLock.ExitWriteLock();
                                    }
                                }
                            }
                        }
                        finally
                        {
                            if (!lockAlreadyHeld)
                                nodeLock.ExitReadLock();
                        }
                        return;
                    }
                    if (k < label.Length)
                        return;
                    i += label.Length;
                }
                finally
                {
                    parentLock.ExitUpgradeableReadLock();
                }
                parent = node;
            }
        }

        private IEnumerable<InternalSearchResult> SearchInternal(TrieNode subtreeRoot, string prefix)
        {
            if (prefix is null)
                throw new ArgumentNullException(nameof(prefix));

            if (!Find(prefix, subtreeRoot, out var node))
                return Enumerable.Empty<InternalSearchResult>();

            // depth-first traversal
            IEnumerable<InternalSearchResult> traverse(TrieNode n, string s)
            {
                if (_values.TryGetValue(n, out var value))
                    yield return new InternalSearchResult(s, n, value);
                var nLock = _locks.GetLock(n);
                var lockAlreadyHeld = nLock.IsReadLockHeld;
                if (!lockAlreadyHeld)
                    nLock.EnterReadLock();
                List<TrieNode> children;
                try
                {
                    // we can't know what is done during enumeration, so we need to make a copy of the children
                    children = n.Children.Values.ToList();
                }
                finally
                {
                    if (!lockAlreadyHeld)
                        nLock.ExitReadLock();
                }
                foreach (var child in children)
                {
                    foreach (var kv in traverse(child, s + child.Label))
                        yield return kv;
                }
            }
            return traverse(node, prefix);
        }
    }
}

// Footnotes:
// 
// 1.   Since we optimistically get a value from a non-threadsafe dictionary without locking,
//      it could end up being null despite TryGetValue returning true, so we need to double-check it
//
