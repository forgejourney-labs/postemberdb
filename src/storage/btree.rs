//! A small, engine-owned B+ tree implementation.
//!
//! Values live only in leaves. Internal nodes contain separator keys and child
//! pointers. Range scans seek to the first matching leaf once, then move to
//! successor leaves with an ancestor stack instead of rescanning subtrees.
//! Nodes are still in memory; page-backed persistence is a separate layer.

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

    pub(crate) fn len(&self) -> usize { self.len }
    pub(crate) fn is_empty(&self) -> bool { self.len == 0 }

    /// Inserts a key or replaces its value. Returns the previous value, if any.
    pub(crate) fn insert(&mut self, key: K, value: V) -> Option<V> {
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

    /// Removes a key and returns its value. Underflow is repaired by borrowing
    /// from a sibling or merging nodes; an unnecessary root level is collapsed.
    pub(crate) fn remove(&mut self, key: &K) -> Option<V> {
        let removed = remove_recursive::<K, V, ORDER>(&mut self.root, key);
        if removed.is_none() {
            return None;
        }

        self.len -= 1;
        loop {
            let collapse = matches!(&*self.root, Node::Internal { keys, .. } if keys.is_empty());
            if !collapse {
                break;
            }
            let previous = std::mem::replace(
                &mut self.root,
                Box::new(Node::Leaf { keys: Vec::new(), values: Vec::new() }),
            );
            if let Node::Internal { mut children, .. } = *previous {
                self.root = children.remove(0);
            }
        }
        removed
    }

    pub(crate) fn contains_key(&self, key: &K) -> bool { self.get(key).is_some() }

    pub(crate) fn clear(&mut self) {
        self.root = Box::new(Node::Leaf { keys: Vec::new(), values: Vec::new() });
        self.len = 0;
    }

    pub(crate) fn get(&self, key: &K) -> Option<&V> {
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
    ///
    /// The initial seek is O(log n). Successor leaves are then found by walking
    /// the saved ancestor path, so a range does not recursively revisit each
    /// internal subtree between adjacent leaves.
    pub(crate) fn scan_range(&self, lower: &K, upper: &K) -> Vec<(K, V)>
    where
        V: Clone,
    {
        let mut entries = Vec::new();
        if lower > upper {
            return entries;
        }

        let mut ancestors: Vec<(&Node<K, V>, usize)> = Vec::new();
        let mut current = self.root.as_ref();

        // Seek directly to the leaf that can contain the lower bound.
        loop {
            match current {
                Node::Leaf { .. } => break,
                Node::Internal { keys, children } => {
                    let child_index = keys.partition_point(|separator| lower >= separator);
                    ancestors.push((current, child_index));
                    current = children[child_index].as_ref();
                }
            }
        }

        loop {
            if let Node::Leaf { keys, values } = current {
                let start = keys.partition_point(|key| key < lower);
                for index in start..keys.len() {
                    if keys[index] > *upper {
                        return entries;
                    }
                    entries.push((keys[index].clone(), values[index].clone()));
                }
            }

            // Find the next subtree to the right, then descend through its
            // leftmost children to the next leaf.
            let mut successor = None;
            while let Some((parent, child_index)) = ancestors.pop() {
                let Node::Internal { children, .. } = parent else {
                    unreachable!("ancestor path contains only internal nodes");
                };
                if child_index + 1 < children.len() {
                    let next_index = child_index + 1;
                    ancestors.push((parent, next_index));
                    let mut next = children[next_index].as_ref();
                    loop {
                        match next {
                            Node::Leaf { .. } => break,
                            Node::Internal { children, .. } => {
                                ancestors.push((next, 0));
                                next = children[0].as_ref();
                            }
                        }
                    }
                    successor = Some(next);
                    break;
                }
            }

            match successor {
                Some(next) => current = next,
                None => break,
            }
        }
        entries
    }

    /// Checks ordering, separator correctness, occupancy and equal leaf depth.
    pub(crate) fn validate(&self) -> Result<(), String> {
        let mut leaf_depth = None;
        validate_node::<K, V, ORDER>(&self.root, true, 0, &mut leaf_depth)?;
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
                insert_recursive::<K, V, ORDER>(children[child_index].as_mut(), key, value);
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

fn remove_recursive<K: Ord + Clone, V, const ORDER: usize>(
    node: &mut Node<K, V>,
    key: &K,
) -> Option<V> {
    match node {
        Node::Leaf { keys, values } => {
            let index = keys.binary_search(key).ok()?;
            keys.remove(index);
            Some(values.remove(index))
        }
        Node::Internal { keys, children } => {
            let child_index = keys.partition_point(|separator| key >= separator);
            let removed = remove_recursive::<K, V, ORDER>(children[child_index].as_mut(), key);
            if removed.is_some() {
                // The child's minimum may have changed even when it did not
                // underflow. Refresh parent separators before borrowing or
                // merging: internal rebalancing moves the parent separator
                // into a child, so using the stale value corrupts that subtree.
                refresh_separators(keys, children);
                if is_underfull(children[child_index].as_ref(), ORDER) {
                    rebalance_child(keys, children, child_index, ORDER);
                    refresh_separators(keys, children);
                }
            }
            removed
        }
    }
}

fn minimum_keys<K, V>(node: &Node<K, V>, order: usize) -> usize {
    match node {
        Node::Leaf { .. } => (order + 1) / 2,
        Node::Internal { .. } => order / 2,
    }
}

fn is_underfull<K, V>(node: &Node<K, V>, order: usize) -> bool {
    let key_count = match node {
        Node::Leaf { keys, .. } | Node::Internal { keys, .. } => keys.len(),
    };
    key_count < minimum_keys(node, order)
}

fn can_lend<K, V>(node: &Node<K, V>, order: usize) -> bool {
    let key_count = match node {
        Node::Leaf { keys, .. } | Node::Internal { keys, .. } => keys.len(),
    };
    key_count > minimum_keys(node, order)
}

fn rebalance_child<K: Ord + Clone, V>(
    keys: &mut Vec<K>,
    children: &mut Vec<Box<Node<K, V>>>,
    child_index: usize,
    order: usize,
) {
    // Prefer borrowing to merging so that parent fan-out remains stable.
    if child_index > 0 && can_lend(children[child_index - 1].as_ref(), order) {
        let (left_siblings, current_and_right) = children.split_at_mut(child_index);
        borrow_from_left(
            left_siblings[child_index - 1].as_mut(),
            current_and_right[0].as_mut(),
            &mut keys[child_index - 1],
        );
        return;
    }

    if child_index + 1 < children.len()
        && can_lend(children[child_index + 1].as_ref(), order)
    {
        let (current_and_left, right_siblings) = children.split_at_mut(child_index + 1);
        borrow_from_right(
            current_and_left[child_index].as_mut(),
            right_siblings[0].as_mut(),
            &mut keys[child_index],
        );
        return;
    }

    if child_index > 0 {
        let separator = keys[child_index - 1].clone();
        {
            let (left_siblings, current_and_right) = children.split_at_mut(child_index);
            merge_nodes(
                left_siblings[child_index - 1].as_mut(),
                current_and_right[0].as_mut(),
                &separator,
            );
        }
        keys.remove(child_index - 1);
        children.remove(child_index);
    } else if children.len() > 1 {
        let separator = keys[0].clone();
        {
            let (current, right_siblings) = children.split_at_mut(1);
            merge_nodes(current[0].as_mut(), right_siblings[0].as_mut(), &separator);
        }
        keys.remove(0);
        children.remove(1);
    }
}

fn borrow_from_left<K: Ord + Clone, V>(
    left: &mut Node<K, V>,
    child: &mut Node<K, V>,
    parent_separator: &mut K,
) {
    match (left, child) {
        (
            Node::Leaf { keys: left_keys, values: left_values },
            Node::Leaf { keys: child_keys, values: child_values },
        ) => {
            let donated_key = left_keys.pop().expect("lending leaf has a key");
            let donated_value = left_values.pop().expect("leaf key has a value");
            child_keys.insert(0, donated_key);
            child_values.insert(0, donated_value);
            *parent_separator = child_keys[0].clone();
        }
        (
            Node::Internal { keys: left_keys, children: left_children },
            Node::Internal { keys: child_keys, children: child_children },
        ) => {
            let promoted = left_keys.pop().expect("lending internal node has a key");
            let old_parent_separator = std::mem::replace(parent_separator, promoted);
            child_keys.insert(0, old_parent_separator);
            child_children.insert(0, left_children.pop().expect("lending internal node has a child"));
        }
        _ => unreachable!("siblings must have the same node type"),
    }
}

fn borrow_from_right<K: Ord + Clone, V>(
    child: &mut Node<K, V>,
    right: &mut Node<K, V>,
    parent_separator: &mut K,
) {
    match (child, right) {
        (
            Node::Leaf { keys: child_keys, values: child_values },
            Node::Leaf { keys: right_keys, values: right_values },
        ) => {
            child_keys.push(right_keys.remove(0));
            child_values.push(right_values.remove(0));
            *parent_separator = right_keys[0].clone();
        }
        (
            Node::Internal { keys: child_keys, children: child_children },
            Node::Internal { keys: right_keys, children: right_children },
        ) => {
            let new_parent_separator = right_keys.remove(0);
            let old_parent_separator = std::mem::replace(parent_separator, new_parent_separator);
            child_keys.push(old_parent_separator);
            child_children.push(right_children.remove(0));
        }
        _ => unreachable!("siblings must have the same node type"),
    }
}

fn merge_nodes<K: Ord + Clone, V>(
    left: &mut Node<K, V>,
    right: &mut Node<K, V>,
    parent_separator: &K,
) {
    match (left, right) {
        (
            Node::Leaf { keys: left_keys, values: left_values },
            Node::Leaf { keys: right_keys, values: right_values },
        ) => {
            left_keys.extend(right_keys.drain(..));
            left_values.extend(right_values.drain(..));
        }
        (
            Node::Internal { keys: left_keys, children: left_children },
            Node::Internal { keys: right_keys, children: right_children },
        ) => {
            left_keys.push(parent_separator.clone());
            left_keys.extend(right_keys.drain(..));
            left_children.extend(right_children.drain(..));
        }
        _ => unreachable!("siblings must have the same node type"),
    }
}

fn refresh_separators<K: Ord + Clone, V>(
    keys: &mut Vec<K>,
    children: &[Box<Node<K, V>>],
) {
    keys.clear();
    for child in children.iter().skip(1) {
        keys.push(
            subtree_min(child.as_ref())
                .expect("every non-root B+ tree child must contain at least one key"),
        );
    }
}

fn subtree_min<K: Clone, V>(node: &Node<K, V>) -> Option<K> {
    match node {
        Node::Leaf { keys, .. } => keys.first().cloned(),
        Node::Internal { children, .. } => children.first().and_then(|child| subtree_min(child)),
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

fn validate_node<K: Ord + Clone, V, const ORDER: usize>(
    node: &Node<K, V>,
    is_root: bool,
    depth: usize,
    leaf_depth: &mut Option<usize>,
) -> Result<Option<(K, K)>, String> {
    match node {
        Node::Leaf { keys, values } => {
            if keys.len() != values.len() { return Err("leaf key/value length mismatch".into()); }
            if keys.len() > ORDER { return Err("leaf exceeds maximum key count".into()); }
            if !is_root && keys.len() < (ORDER + 1) / 2 {
                return Err("underfull leaf node".into());
            }
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
            if keys.len() > ORDER { return Err("internal node exceeds maximum key count".into()); }
            if children.len() != keys.len() + 1 { return Err("internal child count mismatch".into()); }
            if keys.windows(2).any(|pair| pair[0] >= pair[1]) {
                return Err("internal separators are not strictly ordered".into());
            }
            if !is_root && keys.len() < ORDER / 2 { return Err("underfull internal node".into()); }
            if is_root && children.len() < 2 { return Err("internal root has fewer than two children".into()); }

            let mut bounds = Vec::with_capacity(children.len());
            for child in children {
                let child_bounds = validate_node::<K, V, ORDER>(child, false, depth + 1, leaf_depth)?;
                if child_bounds.is_none() {
                    return Err("non-root child subtree is empty".into());
                }
                bounds.push(child_bounds);
            }
            for index in 0..keys.len() {
                let left_max = bounds[index].as_ref().map(|(_, max)| max);
                let right_min = bounds[index + 1].as_ref().map(|(min, _)| min);
                if left_max.is_some_and(|max| max >= &keys[index]) {
                    return Err(format!("internal separator violates left child upper bound at index {index}"));
                }
                if right_min.is_some_and(|min| min != &keys[index]) {
                    return Err(format!("internal separator differs from right child minimum at index {index}"));
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
    use std::collections::BTreeMap;

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

    #[test]
    fn deletion_borrows_merges_and_collapses_the_root() {
        let mut tree = BPlusTree::<i32, i32, 3>::new();
        for key in 0..256 {
            tree.insert(key, key * 10);
        }
        tree.validate().unwrap();

        // Alternating low/high removals exercise both sibling directions and
        // force internal levels to rebalance as leaves merge.
        let mut low = 0;
        let mut high = 255;
        while low <= high {
            assert_eq!(tree.remove(&low), Some(low * 10));
            tree.validate().unwrap();
            if low != high {
                assert_eq!(tree.remove(&high), Some(high * 10));
                tree.validate().unwrap();
            }
            low += 1;
            if high == 0 { break; }
            high -= 1;
        }
        assert!(tree.is_empty());
        assert_eq!(tree.len(), 0);
        tree.validate().unwrap();
        assert_eq!(tree.remove(&1), None);
    }

    #[test]
    fn deterministic_mixed_operations_match_btree_map() {
        let mut tree = BPlusTree::<i32, i64, 4>::new();
        let mut reference = BTreeMap::new();
        let mut state = 0x9e37_79b9_7f4a_7c15u64;

        for step in 0..5_000i64 {
            state = state.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let key = ((state >> 32) % 257) as i32;
            if state & 1 == 0 {
                let value = step * -3;
                assert_eq!(tree.insert(key, value), reference.insert(key, value));
            } else {
                assert_eq!(tree.remove(&key), reference.remove(&key));
            }

            assert_eq!(tree.len(), reference.len());
            tree.validate().unwrap_or_else(|error| panic!("B+ tree invariant failed at step {step} (key {key}, state {state:#x}): {error}"));
            for probe in [0, key, 128, 256] {
                assert_eq!(tree.get(&probe), reference.get(&probe));
            }

            if step % 31 == 0 {
                let lower = (state as u32 % 200) as i32;
                let upper = (lower + 40).min(256);
                let actual = tree.scan_range(&lower, &upper);
                let expected: Vec<_> = reference
                    .range(lower..=upper)
                    .map(|(key, value)| (*key, *value))
                    .collect();
                assert_eq!(actual, expected);
            }
        }
    }

    #[test]
    fn deleting_missing_keys_does_not_change_tree() {
        let mut tree = BPlusTree::<i32, i32, 3>::new();
        for key in 0..50 { tree.insert(key, key); }
        assert_eq!(tree.remove(&500), None);
        assert_eq!(tree.len(), 50);
        tree.validate().unwrap();
    }

    #[test]
    fn range_scan_crosses_many_successor_leaves_after_deletions() {
        let mut tree = BPlusTree::<i32, i32, 3>::new();
        for key in 0..300 { tree.insert(key, key); }
        for key in (0..300).step_by(3) { tree.remove(&key); }
        tree.validate().unwrap();

        let actual = tree.scan_range(&91, &187);
        let expected: Vec<_> = (91..=187)
            .filter(|key| *key % 3 != 0)
            .map(|key| (key, key))
            .collect();
        assert_eq!(actual, expected);
    }
}
