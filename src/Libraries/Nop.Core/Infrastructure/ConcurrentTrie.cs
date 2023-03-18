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
            public readonly ConcurrentDictionary<char, TrieNode> Children = new();

            public TrieNode(string label)
            {
                Label = label;
            }
        }

        private readonly object _lock = new();
        private readonly TrieNode _root;
        private readonly string _prefix;
        private readonly ConcurrentDictionary<TrieNode, TValue> _values = new();

        public IEnumerable<string> Keys => Search(string.Empty).Select(kv => kv.Key);

        /// <summary>
        /// Gets a collection that contains the values in the <see cref="ConcurrentTrie{TValue}" />.
        /// May not return values in the corresponding order as keys in <see cref="ConcurrentTrie{TValue}.Keys" />.
        /// </summary>
        public IEnumerable<TValue> Values => _values.Values;


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

            GetOrAddNode(key, () => value);
        }

        /// <summary>
        /// Clears the trie
        /// </summary>
        public void Clear()
        {
            _root.Children.Clear();
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
                if (_values.TryGetValue(n, out var value))
                    yield return new KeyValuePair<string, TValue>(_prefix + s, value);
                foreach (var child in n.Children.Values)
                {
                    foreach (var kv in traverse(child, s + child.Label))
                        yield return kv;
                }
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
            var node = GetOrAddNode(key, valueFactory);
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
                var rootCopy = new TrieNode(_root.Label);
                foreach (var (key, child) in _root.Children)
                    rootCopy.Children.TryAdd(key, child);
                subtree = new(rootCopy, prefix);
                Clear();
                return true;
            }
            var node = _root;
            TrieNode parent = node;
            var span = prefix.AsSpan()[_prefix.Length..];
            var i = 0;
            char last = default;
            while (i < span.Length)
            {
                last = span[i];
                if (!node.Children.TryGetValue(last, out node))
                    return false;
                var label = node.Label.AsSpan();
                var k = GetCommonPrefixLength(span[i..], label);
                if (k == span.Length - i)
                {
                    if (parent.Children.TryRemove(last, out node))
                    {
                        subtree = new(node, prefix);
                        return true;
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

        private TrieNode GetOrAddNode(string key, Func<TValue> valueFactory)
        {
            var node = _root;
            var suffix = key.AsSpan();
            while (true)
            {
                if (node.Children.TryGetValue(suffix[0], out var nextNode))
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
                    lock(_lock)
                    {
                        var splitNode = new TrieNode(suffix[..i].ToString());
                        node.Children[suffix[0]] = splitNode;
                        nextNode.Label = nextKey[i..].ToString();
                        splitNode.Children[nextKey[i]] = nextNode;
                        if (i == suffix.Length) // nextKey starts with suffix
                            return splitNode;
                        node = new TrieNode(suffix[i..].ToString());
                        _values[node] = valueFactory();
                        splitNode.Children[suffix[i]] = node;
                    }
                    return node;
                }
                node = node.Children.GetOrAdd(suffix[0], new TrieNode(suffix.ToString()));
                _values[node] = valueFactory();
                return node;
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
                if (!node.Children.TryGetValue(suffix[0], out node))
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
            TrieNode parent = node;
            var span = key[_prefix.Length..];
            var i = 0;
            char last = default;
            while (i < span.Length)
            {
                last = span[i];
                if (!node.Children.TryGetValue(last, out node))
                    return;
                var label = node.Label.AsSpan();
                var k = GetCommonPrefixLength(span[i..], label);
                if (k == label.Length && k == span.Length - i)
                {
                    lock(_lock)
                    {
                        _values.TryRemove(node, out _);
                        if (node.Children.Count == 0)
                            parent.Children.TryRemove(last, out _);
                        else if (node.Children.Count == 1)
                        {
                            var child = node.Children.FirstOrDefault().Value;
                            if (child != default)
                            {
                                child.Label = node.Label + child.Label;
                                parent.Children[last] = child;
                            }
                        }
                    }
                    return;
                }
                if (k < label.Length)
                    return;
                i += label.Length;
                parent = node;
            }
        }
    }
}
