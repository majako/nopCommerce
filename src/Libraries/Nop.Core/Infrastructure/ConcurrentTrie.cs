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

        private readonly TrieNode _root = new();

        public IEnumerable<string> Keys => Search(string.Empty);


        public bool TryGetValue(string key, out TValue value)
        {
            if (key is null)
                throw new ArgumentNullException(nameof(key));

            var found = Find(key, out var node) && node.IsTerminal;
            value = found ? node.Value : default;
            return found;
        }

        public void Set(string key, TValue value)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentException($"'{nameof(key)}' cannot be null or empty.", nameof(key));

            var node = _root;
            foreach (var c in key)
                node = node.Children.GetOrAdd(c, _ => new());
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
                    foreach (var s_ in traverse(child, s + c))
                        yield return s_;
                }
            }
            return traverse(node, prefix);
        }

        public bool TryRemove(string key)
        {
            return TryRemove(_root, key);
        }

        public TValue GetOrAdd(string key, Func<TValue> valueFactory)
        {
            
        }

        public void Prune(string prefix)
        {
            if (string.IsNullOrEmpty(prefix))
                throw new ArgumentException($"'{nameof(prefix)}' cannot be null or empty.", nameof(prefix));

            var node = _root;
            TrieNode parent = null;
            char last = default;
            foreach (var c in prefix)
            {
                parent = node;
                if (!node.Children.TryGetValue(c, out node))
                    return;
                last = c;
            }
            parent?.Children.TryRemove(last, out _);
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
