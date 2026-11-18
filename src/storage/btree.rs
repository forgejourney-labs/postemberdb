//! A small, engine-owned B+ tree implementation.
//!
//! Values live only in leaves. Internal nodes contain separator keys and child
//! pointers. This module deliberately does not wrap std::collections::BTreeMap.
//! The first milestone supports insertion, point lookup, bounded scans and
//! structural validation; deletion and disk-page persistence are separate work.

#[derive(Debug, Clone)]
enum Node<K, V> {
    Leaf { keys: Vec<K>, values: Vec<V> },
    Internal { keys: Vec<K>, children: Vec<Box<Node<K, V>>> },
}

/// An in-memory B+ tree. ORDER is the maximum number of keys per node.
#[derive(Debug, Clone)]
pub(crate) struct BPlusTree<K, V, const ORDER: usize = 32> {
    root: Box<Node<K, V>>,
    len: usize,
}

impl<K: Ord + Clone, V, const ORDER: usize> BPlusTree<K, V, ORDER> {
    pub(crate) fn new() -> Self {
        assert!(ORDER >= 3, "B+ tree order must be at least 3");
        Self {
            root: Box::new(Node::Leaf { keys: Vec::new(), values: Vec::new() }),
            len: 0,
        }
    }

    pub(super) fn len(&self) -> usize { self.len }
    pub(super) fn is_empty(&self) -> bool { self.len == 0 }

    /// Inserts a key or replaces its value. Returns the previous value, if any.
    pub(super) fn insert(&mut self, key: K, value: V) -> Option<V> {
        let (previous, split) = insert_recursive::<K, V, ORDER>(&mut self.root, key, value);
        if let Some((separator, right)) = split {
            let left = std::mem::replace(
                &mut self.root,
                Box::new(Node::Leaf { keys: Vec::new(), values: Vec::new() }),
            );
            self.root = Box::new(Node::Internal {
                keys: vec![separator],
                children: vec![left, right],
            });
        }
        if previous.is_none() { self.len += 1; }
        previous
    }

    pub(super) fn get(&self, key: &K) -> Option<&V> {
        let mut node = self.root.as_ref();
        loop {
            match node {
                Node::Leaf { keys, values } => {
                    return keys.binary_search(key).ok().map(|index| &values[index]);
                }
                Node::Internal { keys, children } => {
                    let child = keys.partition_point(|separator| key >= separator);
                    node = children[child].as_ref();
                }
            }
        }
    }

    /// Returns key/value pairs in the inclusive range, in ascending key order.
    pub(super) fn scan_range(&self, lower: &K, upper: &K) -> Vec<(K, V)>
    where
        V: Clone,
    {
        let mut entries = Vec::new();
        collect_entries(&self.root, &mut entries);
        entries.into_iter()
            .filter(|(key, _)| key >= lower && key <= upper)
            .collect()
    }

    /// Checks ordering, separator correctness, occupancy and equal leaf depth.
    pub(super) fn validate(&self) -> Result<(), String> {
        let mut leaf_depth = None;
        validate_node(&self.root, true, 0, &mut leaf_depth)?;
        let mut count = 0;
        count_entries(&self.root, &mut count);
        if count != self.len {
            return Err(format!("tree length mismatch: expected {}, found {count}", self.len));
        }
        Ok(())
    }
}

fn insert_recursive<K: Ord + Clone, V, const ORDER: usize>(
    node: &mut Node<K, V>,
    key: K,
    value: V,
) -> (Option<V>, Option<(K, Box<Node<K, V>>)>) {
    match node {
        Node::Leaf { keys, values } => {
            match keys.binary_search(&key) {
                Ok(index) => return (Some(std::mem::replace(&mut values[index], value)), None),
                Err(index) => {
                    keys.insert(index, key);
                    values.insert(index, value);
                }
            }
            if keys.len() <= ORDER { return (None, None); }

            let midpoint = keys.len() / 2;
            let right_keys = keys.split_off(midpoint);
            let right_values = values.split_off(midpoint);
            let separator = right_keys[0].clone();
            let right = Box::new(Node::Leaf { keys: right_keys, values: right_values });
            (None, Some((separator, right)))
        }
        Node::Internal { keys, children } => {
            let child_index = keys.partition_point(|separator| key >= *separator);
            let (previous, child_split) =
                insert_recursive::<K, V, ORDER>(&mut children[child_index], key, value);
            if let Some((separator, right_child)) = child_split {
                keys.insert(child_index, separator);
                children.insert(child_index + 1, right_child);
            } else {
                return (previous, None);
            }
            if keys.len() <= ORDER { return (previous, None); }

            let midpoint = keys.len() / 2;
            let right_keys = keys.split_off(midpoint + 1);
            let separator = keys.pop().expect("overflowing internal node has a middle key");
            let right_children = children.split_off(midpoint + 1);
            let right = Box::new(Node::Internal { keys: right_keys, children: right_children });
            (previous, Some((separator, right)))
        }
    }
}

fn collect_entries<K: Clone, V: Clone>(node: &Node<K, V>, out: &mut Vec<(K, V)>) {
    match node {
        Node::Leaf { keys, values } => out.extend(keys.iter().cloned().zip(values.iter().cloned())),
        Node::Internal { children, .. } => {
            for child in children { collect_entries(child, out); }
        }
    }
}

fn count_entries<K, V>(node: &Node<K, V>, count: &mut usize) {
    match node {
        Node::Leaf { keys, .. } => *count += keys.len(),
        Node::Internal { children, .. } => {
            for child in children { count_entries(child, count); }
        }
    }
}

fn validate_node<K: Ord + Clone, V>(
    node: &Node<K, V>,
    is_root: bool,
    depth: usize,
    leaf_depth: &mut Option<usize>,
) -> Result<Option<(K, K)>, String> {
    match node {
        Node::Leaf { keys, values } => {
            if keys.len() != values.len() { return Err("leaf key/value length mismatch".into()); }
            if keys.windows(2).any(|pair| pair[0] >= pair[1]) {
                return Err("leaf keys are not strictly ordered".into());
            }
            match leaf_depth {
                Some(expected) if *expected != depth => return Err("leaves have different depths".into()),
                None => *leaf_depth = Some(depth),
                _ => {}
            }
            Ok(keys.first().cloned().zip(keys.last().cloned()))
        }
        Node::Internal { keys, children } => {
            if children.len() != keys.len() + 1 { return Err("internal child count mismatch".into()); }
            if keys.windows(2).any(|pair| pair[0] >= pair[1]) {
                return Err("internal separators are not strictly ordered".into());
            }
            if !is_root && keys.is_empty() { return Err("underfull internal node".into()); }
            if is_root && children.len() < 2 { return Err("internal root has fewer than two children".into()); }

            let mut bounds = Vec::with_capacity(children.len());
            for child in children {
                bounds.push(validate_node(child, false, depth + 1, leaf_depth)?);
            }
            for index in 0..keys.len() {
                let left_max = bounds[index].as_ref().map(|(_, max)| max);
                let right_min = bounds[index + 1].as_ref().map(|(min, _)| min);
                if left_max.is_some_and(|max| max >= &keys[index])
                    || right_min.is_some_and(|min| min != &keys[index]) {
                    return Err("internal separator does not match child key ranges".into());
                }
            }
            let min = bounds.first().and_then(|bound| bound.as_ref()).map(|(min, _)| min.clone());
            let max = bounds.last().and_then(|bound| bound.as_ref()).map(|(_, max)| max.clone());
            Ok(min.zip(max))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BPlusTree;

    #[test]
    fn splits_multiple_levels_and_scans_sorted_ranges() {
        let mut tree = BPlusTree::<i32, String, 3>::new();
        for key in (0..500).rev() {
            assert_eq!(tree.insert(key, format!("value-{key}")), None);
        }
        assert_eq!(tree.len(), 500);
        for key in 0..500 {
            assert_eq!(tree.get(&key), Some(&format!("value-{key}")));
        }
        let range = tree.scan_range(&120, &130);
        assert_eq!(range.len(), 11);
        assert_eq!(range.first().unwrap().0, 120);
        assert_eq!(range.last().unwrap().0, 130);
        tree.validate().unwrap();
    }

    #[test]
    fn replaces_existing_values_without_changing_length() {
        let mut tree = BPlusTree::<i32, &str, 4>::new();
        assert_eq!(tree.insert(7, "first"), None);
        assert_eq!(tree.insert(7, "second"), Some("first"));
        assert_eq!(tree.get(&7), Some(&"second"));
        assert_eq!(tree.len(), 1);
        tree.validate().unwrap();
    }

    #[test]
    fn empty_tree_and_single_leaf_are_valid() {
        let mut tree = BPlusTree::<i32, i32, 8>::new();
        assert!(tree.is_empty());
        tree.validate().unwrap();
        tree.insert(1, 10);
        tree.validate().unwrap();
    }
}
