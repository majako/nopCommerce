using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Nop.Core.Infrastructure
{
    public class ConcurrentTrie<TValue>
    {
        private class TrieNode
        {
            public ConcurrentDictionary<char, TrieNode> Children = new();
            public bool IsTerminal = false;
            public TValue Value;
        }

        private readonly TrieNode _root;
        private readonly string _prefix;

        public IEnumerable<string> Keys => Search(string.Empty).Select(kv => kv.Key);
        public IEnumerable<TValue> Values => Search(string.Empty).Select(kv => kv.Value);


        public ConcurrentTrie() : this(new(), string.Empty)
        {
        }

        private ConcurrentTrie(TrieNode root, string prefix)
        {
            _root = root;
            _prefix = prefix;
        }


        public bool TryGetValue(string key, out TValue value)
        {
            if (key is null)
                throw new ArgumentNullException(nameof(key));

            var found = Find(key.ToLowerInvariant(), out var node) && node.IsTerminal;
            value = found ? node.Value : default;
            return found;
        }

        public void Set(string key, TValue value)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentException($"'{nameof(key)}' cannot be null or empty.", nameof(key));

            var node = GetOrAddNode(key.ToLowerInvariant());
            node.Value = value;
            node.IsTerminal = true;
        }

        public void Clear()
        {
            _root.Children.Clear();
        }

        public IEnumerable<KeyValuePair<string, TValue>> Search(string prefix)
        {
            if (prefix is null)
                throw new ArgumentNullException(nameof(prefix));

            if (!Find(prefix, out var node))
                return Enumerable.Empty<KeyValuePair<string, TValue>>();

            IEnumerable<KeyValuePair<string, TValue>> traverse(TrieNode n, string s)
            {
                if (n.IsTerminal)
                    yield return new KeyValuePair<string, TValue>(_prefix + s, n.Value);
                foreach (var (c, child) in n.Children)
                {
                    foreach (var kv in traverse(child, s + c))
                        yield return kv;
                }
            }
            return traverse(node, prefix.ToLowerInvariant());
        }

        public bool TryRemove(string key)
        {
            return TryRemove(_root, key.ToLowerInvariant());
        }

        public TValue GetOrAdd(string key, Func<TValue> valueFactory)
        {
            var node = GetOrAddNode(key.ToLowerInvariant());
            if (!node.IsTerminal)
                node.Value = valueFactory();
            node.IsTerminal = true;
            return node.Value;
        }

        public bool Prune(string prefix, out ConcurrentTrie<TValue> subtree)
        {
            if (string.IsNullOrEmpty(prefix))
                throw new ArgumentException($"'{nameof(prefix)}' cannot be null or empty.", nameof(prefix));

            subtree = default;
            var node = _root;
            TrieNode parent = null;
            char last = default;
            foreach (var c in prefix.ToLowerInvariant())
            {
                parent = node;
                if (!node.Children.TryGetValue(c, out node))
                    return false;
                last = c;
            }
            if (parent?.Children.TryRemove(last, out var subtreeRoot) == true)
                subtree = new ConcurrentTrie<TValue>(subtreeRoot, prefix);
            return true;
        }

        private TrieNode GetOrAddNode(string key)
        {
            var node = _root;
            foreach (var c in key)
                node = node.Children.GetOrAdd(c, _ => new());
            return node;
        }

        private bool Find(string key, out TrieNode node)
        {
            node = _root;
            foreach (var c in key)
            {
                if (!node.Children.TryGetValue(c, out node))
                    return false;
            }
            return true;
        }

        private bool TryRemove(TrieNode node, string key)
        {
            if (key.Length == 0)
            {
                if (node.IsTerminal)
                {
                    node.IsTerminal = false;
                    node.Value = default;
                }
                return !node.Children.IsEmpty;
            }
            var c = key[0];
            if (node.Children.TryGetValue(c, out var child))
            {
                if (!TryRemove(child, key[1..]))
                    node.Children.TryRemove(new(c, child));
            }
            return true;
        }
    }
}
