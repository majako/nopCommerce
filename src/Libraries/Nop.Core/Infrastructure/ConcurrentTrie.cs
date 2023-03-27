using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
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
            private record ValueWrapper(TValue Value);

            private static readonly ValueWrapper _deleted = new(default);
            public readonly string Label;
            public readonly Dictionary<char, TrieNode> Children = new();
            private volatile ValueWrapper _value;

            public bool IsDeleted => _value == _deleted;
            public bool HasValue => _value != null;

            public TrieNode(string label = "")
            {
                Label = label;
            }

            public TrieNode(string label, TrieNode node) : this(label)
            {
                Children = node.Children;
                _value = node._value;
            }

            public bool TryGetValue(out TValue value)
            {
                var wrapper = _value;
                value = default;
                if (wrapper == null)
                    return false;
                value = wrapper.Value;
                return true;
            }

            public bool TryRemoveValue(out TValue value)
            {
                var wrapper = Interlocked.Exchange(ref _value, null);
                if (wrapper == null)
                {
                    value = default;
                    return false;
                }
                value = wrapper.Value;
                return true;
            }

            public void SetValue(TValue value)
            {
                _value = new(value);
            }

            public TValue GetOrAddValue(TValue value)
            {
                var wrapper = Interlocked.CompareExchange(ref _value, new(value), null);
                return wrapper != null ? wrapper.Value : value;
            }

            public void Delete()
            {
                _value = _deleted;
            }
        }

        private record struct InternalSearchResult(string Key, TrieNode Node, TValue Value);

        private volatile TrieNode _root = new();
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
        /// <param name="key">The key of the item to get (case-sensitive)</param>
        /// <param name="value">The value associated with <paramref name="key"/>, if found</param>
        /// <returns>
        /// True if the key was found, otherwise false
        /// </returns>
        public bool TryGetValue(string key, out TValue value)
        {
            if (key is null)
                throw new ArgumentNullException(nameof(key));

            value = default;
            return Find(key, _root, out var node) && node.TryGetValue(out value);
        }

        /// <summary>
        /// Adds a key-value pair to the trie
        /// </summary>
        /// <param name="key">The key of the new item (case-sensitive)</param>
        /// <param name="value">The value to be associated with <paramref name="key"/></param>
        public void Add(string key, TValue value)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentException($"'{nameof(key)}' cannot be null or empty.", nameof(key));

            GetOrAddNode(key, value, true);
        }

        /// <summary>
        /// Clears the trie
        /// </summary>
        public void Clear()
        {
            _root = new();
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
        public TValue GetOrAdd(string key, TValue value)
        {
            // the value is already set when we get the node if it already exists, but we call GetOrAddValue anyway to get the value
            return GetOrAddNode(key, value).GetOrAddValue(value);
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
                var parentLock = GetLock(parent);
                parentLock.EnterUpgradeableReadLock();
                try
                {
                    if (!parent.Children.TryGetValue(c, out node))
                        return false;
                    var label = node.Label.AsSpan();
                    var k = GetCommonPrefixLength(span[i..], label);
                    if (k == span.Length - i)
                    {
                        parentLock.EnterWriteLock();
                        try
                        {
                            if (parent.Children.Remove(c, out node))
                            {
                                subtree = new(new TrieNode(prefix[..i] + node.Label, node));
                                return true;
                            }
                        }
                        finally { parentLock.ExitWriteLock(); }
                        return false;   // was removed by another thread
                    }
                    if (k < label.Length)
                        return false;
                    i += label.Length;
                }
                finally { parentLock.ExitUpgradeableReadLock(); }
                parent = node;
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetCommonPrefixLength(ReadOnlySpan<char> s1, ReadOnlySpan<char> s2)
        {
            var i = 0;
            var minLength = Math.Min(s1.Length, s2.Length);
            for (; i < minLength && s2[i] == s1[i]; i++)
                ;
            return i;
        }

        /// <summary>
        /// Gets a lock on the node's children
        /// </summary>
        /// <remarks>
        /// May return the same lock for two different nodes, so the user needs to check to avoid lock recursion exceptions
        /// </remarks>
        private ReaderWriterLockSlim GetLock(TrieNode node)
        {
            return _locks.GetLock(node.Children);
        }

        private bool Find(string key, TrieNode subtreeRoot, out TrieNode node)
        {
            node = subtreeRoot;
            if (key.Length == 0)
                return true;
            var suffix = key.AsSpan();
            while (true)
            {
                var nodeLock = GetLock(node);
                nodeLock.EnterReadLock();
                try
                {
                    if (!node.Children.TryGetValue(suffix[0], out node))
                        return false;
                }
                finally { nodeLock.ExitReadLock(); }
                var span = node.Label.AsSpan();
                var i = GetCommonPrefixLength(suffix, span);
                if (i == span.Length)
                {
                    if (i == suffix.Length)
                        return node.HasValue;
                    suffix = suffix[i..];
                    continue;
                }
                return false;
            }
        }

        private TrieNode GetOrAddNode(string key, TValue value, bool overwrite = false)
        {
            var node = _root;
            var suffix = key.AsSpan();
            ReaderWriterLockSlim nodeLock;
            char c;
            while (true)
            {
                c = suffix[0];
                nodeLock = GetLock(node);
                nodeLock.EnterUpgradeableReadLock();
                try
                {
                    if (node.Children.TryGetValue(c, out var nextNode))
                    {
                        var label = nextNode.Label.AsSpan();
                        var i = GetCommonPrefixLength(label, suffix);
                        if (i == label.Length)   // suffix starts with label
                        {
                            if (i == suffix.Length)    // keys are equal - this is the node we're looking for
                            {
                                if (overwrite)
                                    nextNode.SetValue(value);
                                else
                                    nextNode.GetOrAddValue(value);
                                return nextNode;
                            }
                            // advance the suffix and continue the search from nextNode
                            suffix = suffix[label.Length..];
                            node = nextNode;
                            continue;
                        }

                        var splitNode = new TrieNode(suffix[..i].ToString());
                        splitNode.Children[label[i]] = new TrieNode(label[i..].ToString(), nextNode);
                        TrieNode outNode;
                        if (i == suffix.Length) // label starts with suffix, so we can return splitNode
                            outNode = splitNode;
                        else    // the keys diverge, so we need to branch from splitNode
                            splitNode.Children[suffix[i]] = outNode = new TrieNode(suffix[i..].ToString());
                        outNode.SetValue(value);
                        nodeLock.EnterWriteLock();
                        try
                        {
                            node.Children[c] = splitNode;
                        }
                        finally { nodeLock.ExitWriteLock(); }
                        return outNode;
                    }
                    // if there is no child starting with c, we can just add and return one
                    nodeLock.EnterWriteLock();
                    try
                    {
                        var suffixNode = new TrieNode(suffix.ToString());
                        suffixNode.SetValue(value);
                        return node.Children[c] = suffixNode;
                    }
                    finally { nodeLock.ExitWriteLock(); }
                }
                finally { nodeLock.ExitUpgradeableReadLock(); }
            }
        }

        private void Remove(TrieNode subtreeRoot, ReadOnlySpan<char> key)
        {
            TrieNode node, grandparent = null;
            var parent = subtreeRoot;
            var i = 0;
            while (i < key.Length)
            {
                var c = key[i];
                var parentLock = GetLock(parent);
                parentLock.EnterReadLock();
                try
                {
                    if (!parent.Children.TryGetValue(c, out node))
                        return;
                }
                finally { parentLock.ExitReadLock(); }
                var label = node.Label.AsSpan();
                var k = GetCommonPrefixLength(key[i..], label);
                if (k == label.Length && k == key.Length - i)   // is this the node we're looking for?
                {
                    if (node.TryRemoveValue(out _))
                    {
                        var nodeLock = GetLock(node);
                        var grandparentLock = grandparent != null ? GetLock(grandparent) : null;
                        var lockAlreadyHeld = nodeLock == parentLock || nodeLock == grandparentLock;
                        if (!lockAlreadyHeld)
                            nodeLock.EnterReadLock();
                        else
                            nodeLock.EnterUpgradeableReadLock();
                        try
                        {
                            var nChildren = node.Children.Count;
                            if (nChildren == 0) // if the node has no children, we can just remove it
                            {
                                parentLock.EnterWriteLock();
                                try
                                {
                                    if (!parent.Children.TryGetValue(c, out var n) || n != node)
                                        return;  // was removed or replaced by another thread
                                    parent.Children.Remove(c);
                                    node.Delete();
                                    if (parent.Children.Count == 1 && grandparent != null && !parent.HasValue)
                                    {
                                        var grandparentLockAlreadyHeld = grandparentLock == parentLock;
                                        if (!grandparentLockAlreadyHeld)
                                            grandparentLock.EnterWriteLock();
                                        try
                                        {
                                            c = parent.Label[0];
                                            if (!grandparent.Children.TryGetValue(c, out n) || n != parent || parent.HasValue)
                                                break;
                                            var child = parent.Children.First().Value;
                                            grandparent.Children[c] = new TrieNode(parent.Label + child.Label, child);
                                            parent.Delete();
                                        }
                                        finally
                                        {
                                            if (!grandparentLockAlreadyHeld)
                                                grandparentLock.ExitWriteLock();
                                        }
                                    }
                                    return;
                                }
                                finally { parentLock.ExitWriteLock(); }
                            }
                            else if (nChildren == 1)    // if there is a single child, we can merge it with node
                            {
                                parentLock.EnterWriteLock();
                                try
                                {
                                    if (!parent.Children.TryGetValue(c, out var n) || n != node)
                                        break;  // was removed or replaced by another thread
                                    var child = node.Children.First().Value;
                                    parent.Children[c] = new TrieNode(node.Label + child.Label, child);
                                    node.Delete();
                                    return;
                                }
                                finally { parentLock.ExitWriteLock(); }
                            }
                            return; // the node is either already removed, or it is a branching node
                        }
                        finally
                        {
                            if (!lockAlreadyHeld)
                                nodeLock.ExitReadLock();
                            else
                                nodeLock.ExitUpgradeableReadLock();
                        }
                    }
                }
                if (k < label.Length)
                    return;
                i += label.Length;
                grandparent = parent;
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
                if (n.TryGetValue(out var value))
                    yield return new InternalSearchResult(s, n, value);
                var nLock = GetLock(n);
                nLock.EnterReadLock();
                List<TrieNode> children;
                try
                {
                    // we can't know what is done during enumeration, so we need to make a copy of the children
                    children = n.Children.Values.ToList();
                }
                finally { nLock.ExitReadLock(); }
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
