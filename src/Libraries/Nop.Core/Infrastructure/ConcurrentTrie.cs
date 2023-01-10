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
            public ConcurrentDictionary<char, Lazy<TrieNode>> Children = new();
            public bool IsTerminal = false;
            public TValue Value;
        }

        private readonly TrieNode _root;

        public IEnumerable<string> Keys => Search(string.Empty);


        public ConcurrentTrie() : this(new())
        {
        }

        private ConcurrentTrie(TrieNode root)
        {
            _root = root;
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

            var node = GetOrAddNode(key.ToLowerInvariant()).Value;
            node.Value = value;
            node.IsTerminal = true;
        }

        public void Clear()
        {
            _root.Children.Clear();
        }

        public IEnumerable<string> Search(string prefix)
        {
            if (prefix is null)
                throw new ArgumentNullException(nameof(prefix));

            if (!Find(prefix, out var node))
                return Enumerable.Empty<string>();

            static IEnumerable<string> traverse(TrieNode n, string s)
            {
                if (n.IsTerminal)
                    yield return s;
                foreach (var (c, child) in n.Children)
                {
                    foreach (var s_ in traverse(child.Value, s + c))
                        yield return s_;
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
            if (!node.IsValueCreated)
            {
                node.Value.Value = valueFactory();
                node.Value.IsTerminal = true;
            }
            return node.Value.Value;
        }

        public bool Prune(string prefix, out ConcurrentTrie<TValue> subtree)
        {
            if (string.IsNullOrEmpty(prefix))
                throw new ArgumentException($"'{nameof(prefix)}' cannot be null or empty.", nameof(prefix));

            subtree = default;
            var node = new Lazy<TrieNode>(_root);
            TrieNode parent = null;
            char last = default;
            foreach (var c in prefix.ToLowerInvariant())
            {
                parent = node.Value;
                if (!node.Value.Children.TryGetValue(c, out node))
                    return false;
                last = c;
            }
            if (parent?.Children.TryRemove(last, out var subtreeRoot) == true)
                subtree = new ConcurrentTrie<TValue>(subtreeRoot.Value);
            return true;
        }

        private Lazy<TrieNode> GetOrAddNode(string key)
        {
            var node = new Lazy<TrieNode>(_root);
            foreach (var c in key)
                node = node.Value.Children.GetOrAdd(c, _ => new(true));
            return node;
        }

        private bool Find(string key, out TrieNode node)
        {
            node = default;
            var lazyNode = new Lazy<TrieNode>(_root);
            foreach (var c in key)
            {
                if (!lazyNode.Value.Children.TryGetValue(c, out lazyNode))
                    return false;
            }
            node = lazyNode.Value;
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
                if (!TryRemove(child.Value, key[1..]))
                    node.Children.TryRemove(new(c, child));
            }
            return true;
        }
    }
}
