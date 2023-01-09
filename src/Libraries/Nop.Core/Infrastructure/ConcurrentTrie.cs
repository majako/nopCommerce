using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Nop.Core.Infrastructure
{
    public class ConcurrentTrie<TValue>// : IDictionary<string, TValue>
    {
        private class TrieNode
        {
            public ConcurrentDictionary<char, TrieNode> Children = new();
            public bool IsTerminal = false;
            public TValue Value;

            public TrieNode()
            {
            }
        }

        private readonly TrieNode _root = new();

        public bool TryGetValue(string key, out TValue value)
        {
            var found = Find(key, out var node) && node.IsTerminal;
            value = found ? node.Value : default;
            return found;
        }

        public void Insert(string key, TValue value)
        {
            var node = _root;
            foreach (var c in key)
                node = node.Children.GetOrAdd(c, _ => new());
            node.Value = value;
            node.IsTerminal = true;
        }

        public IEnumerable<string> Search(string key)
        {
            if (!Find(key, out var node))
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
            return traverse(node, key);
        }

        public bool TryRemove(string key)
        {
            return TryRemove(_root, key);
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
