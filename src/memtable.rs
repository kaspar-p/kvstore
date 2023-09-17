use std::{cmp::Ordering, fmt::Display, ptr::NonNull};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
enum Color {
    Red,
    Black,
}

impl Display for Color {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            Color::Black => f.write_str("Black"),
            Color::Red => f.write_str("Red"),
        }
    }
}

#[derive(Debug)]
struct Node<K: Ord + Eq + std::fmt::Debug, V> {
    key: K,
    value: V,
    color: Color,

    parent: NodeLink<K, V>,
    left: NodeLink<K, V>,
    right: NodeLink<K, V>,
}

impl<K: Ord + Eq + std::fmt::Debug, V: std::fmt::Debug> Display for Node<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        return f.write_fmt(format_args!(
            "[{}] {:#?}={:#?}",
            self.color, self.key, self.value
        ));
    }
}

impl<K: Ord + Eq + std::fmt::Debug, V> PartialEq for Node<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

#[derive(Debug)]
struct NodeLink<K: Ord + Eq + std::fmt::Debug, V> {
    inner: Option<NonNull<Node<K, V>>>,
}

impl<K: Ord + Eq + std::fmt::Debug, V> NodeLink<K, V> {
    pub fn new(key: K, value: V) -> Self {
        unsafe {
            let new_node = NonNull::new_unchecked(Box::into_raw(Box::new(Node {
                key,
                value,
                color: Color::Black,
                parent: NodeLink::none(),
                left: NodeLink::none(),
                right: NodeLink::none(),
            })));

            NodeLink {
                inner: Some(new_node),
            }
        }
    }

    pub fn none() -> NodeLink<K, V> {
        NodeLink { inner: None }
    }

    pub fn is_none(&self) -> bool {
        return self.inner.is_none();
    }

    pub fn is_some(&self) -> bool {
        return self.inner.is_some();
    }

    pub fn get_key(&self) -> Option<&K> {
        unsafe { self.inner.map(|node| &(*node.as_ptr()).key) }
    }

    pub fn get_value(&self) -> Option<&V> {
        unsafe { Some(&(*self.inner.unwrap().as_ptr()).value) }
    }

    /// Get the color of a Link (pointer-to-node)
    /// Note: A sentinel value (pointer to nothing) has color Black
    pub fn get_color(&self) -> Color {
        unsafe {
            match &self.inner {
                None => Color::Black,
                Some(node) => (*node.as_ptr()).color,
            }
        }
    }

    pub fn is_red(&self) -> bool {
        match self.get_color() {
            Color::Red => true,
            Color::Black => false,
        }
    }

    pub fn set_color(&self, color: Color) {
        unsafe {
            match &self.inner {
                None => return,
                Some(node) => {
                    (*node.as_ptr()).color = color;
                }
            }
        }
    }

    pub fn get_left_child(&self) -> NodeLink<K, V> {
        unsafe {
            match &self.inner {
                None => NodeLink::none(),
                Some(node) => (*node.as_ptr()).left.clone(),
            }
        }
    }

    pub fn set_left_child(&mut self, child: NodeLink<K, V>) {
        unsafe {
            self.inner.map(|node| {
                (*node.as_ptr()).left = child.clone();
            });
        }
    }

    pub fn get_right_child(&self) -> NodeLink<K, V> {
        unsafe {
            match &self.inner {
                None => NodeLink::none(),
                Some(node) => (*node.as_ptr()).right.clone(),
            }
        }
    }

    pub fn set_right_child(&mut self, child: NodeLink<K, V>) {
        unsafe {
            self.inner.map(|node| {
                (*node.as_ptr()).right = child.clone();
            });
        }
    }

    pub fn get_parent(&self) -> NodeLink<K, V> {
        unsafe {
            match &self.inner {
                None => NodeLink::none(),
                Some(node) => (*node.as_ptr()).parent.clone(),
            }
        }
    }

    pub fn set_parent(&mut self, parent: NodeLink<K, V>) {
        unsafe {
            self.inner.map(|node| {
                (*node.as_ptr()).parent = parent;
            });
        }
    }
}

impl<K: Ord + Eq + std::fmt::Debug, V> PartialEq for NodeLink<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<K: Ord + Eq + std::fmt::Debug, V> Eq for NodeLink<K, V> {} // Derives from PartialEq

impl<K: Ord + Eq + std::fmt::Debug, V> PartialOrd for NodeLink<K, V> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        unsafe {
            match &self.inner {
                None => None,
                Some(node) => match &other.inner {
                    None => None,
                    Some(other) => {
                        return Some((*node.as_ptr()).key.cmp(&(*other.as_ptr()).key));
                    }
                },
            }
        }
    }
}

impl<K: Ord + Eq + std::fmt::Debug, V> Ord for NodeLink<K, V> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        unsafe {
            return (*self.inner.as_ref().unwrap().as_ptr())
                .key
                .cmp(&(*other.inner.unwrap().as_ptr()).key);
        }
    }
}

impl<K: Ord + Eq + std::fmt::Debug, V> Clone for NodeLink<K, V> {
    fn clone(&self) -> NodeLink<K, V> {
        NodeLink {
            inner: self.inner.clone(),
        }
    }
}

pub struct Memtable<K: Ord + Eq + std::fmt::Debug, V> {
    memtable_size: i32,
    root: NodeLink<K, V>,
}

pub enum PutError {}

impl<K: Ord + Eq + std::fmt::Debug, V: std::fmt::Debug> Memtable<K, V> {
    pub fn new(memtable_size: i32) -> Self {
        Memtable {
            memtable_size,
            root: NodeLink::none(),
        }
    }

    /// For the start of a an rb_insert, we need to traverse the "looking" node
    /// starting at the root, in accordance to `node.key` and its ordering.
    fn rb_insert_traverse(&self, node: NodeLink<K, V>) -> NodeLink<K, V> {
        let mut last_head = NodeLink::none();
        let mut traverse_head = self.root.clone();
        while !traverse_head.is_none() {
            last_head = traverse_head.clone();
            match node.cmp(&traverse_head) {
                Ordering::Less => traverse_head = traverse_head.get_left_child(),
                _ => traverse_head = traverse_head.get_right_child(), // Other cases
            }
        }
        return last_head;
    }

    /// For an optional parent and a child, place the child within the parent according to the keys
    /// of the parent and child.
    fn rb_insert_place_child(&mut self, mut parent: NodeLink<K, V>, child: NodeLink<K, V>) {
        if parent.is_none() {
            self.root = child;
        } else {
            match child.cmp(&parent) {
                Ordering::Less => parent.set_left_child(child),
                _ => parent.set_right_child(child),
            }
        }
    }

    /// Adapted from CLRS page 315, thanks to my roommate Eamon for letting me borrow it.
    fn rb_insert(&mut self, mut z: NodeLink<K, V>) -> () {
        println!("Traversing insert!");
        let insert_parent = self.rb_insert_traverse(z.clone());

        println!("Set parent!");
        z.set_parent(insert_parent.clone());

        self.rb_insert_place_child(insert_parent.clone(), z.clone());

        z.set_left_child(NodeLink::none());
        z.set_right_child(NodeLink::none());
        z.set_color(Color::Red);

        println!("Before fixup!");
        self.rb_fixup(z);
    }

    /// From CLRS page 313, thanks to my roommate Eamon for letting me borrow it.
    fn left_rotate(&mut self, mut x: NodeLink<K, V>) {
        let mut y = x.get_right_child();
        x.set_right_child(y.get_left_child());
        if y.get_left_child().is_some() {
            y.get_left_child().set_parent(x.clone());
        }
        y.set_parent(x.get_parent());
        if x.get_parent().is_none() {
            self.root = y.clone();
        } else if x == x.get_parent().get_left_child() {
            x.get_parent().set_left_child(y.clone());
        } else {
            x.get_parent().set_right_child(y.clone());
        }
        y.set_left_child(x.clone());
        x.set_parent(y.clone());
    }

    fn right_rotate(&mut self, mut x: NodeLink<K, V>) {
        let mut y = x.get_left_child();
        x.set_left_child(y.get_right_child());
        if y.get_right_child().is_some() {
            y.get_right_child().set_parent(x.clone());
        }
        y.set_parent(x.get_parent());
        if x.get_parent().is_none() {
            self.root = y.clone();
        } else if x == x.get_parent().get_right_child() {
            x.get_parent().set_right_child(y.clone());
        } else {
            x.get_parent().set_left_child(y.clone());
        }
        y.set_right_child(x.clone());
        x.set_parent(y.clone());
    }

    /// Adapted from CLRS page 316, thanks to my roommate Eamon for letting me borrow it.
    fn rb_fixup(&mut self, mut z: NodeLink<K, V>) {
        while z.get_parent().is_red() {
            println!(
                "Still red: {:#?}, {:#?}, {:#?}, {:#?}, {:#?}",
                z,
                z.get_parent(),
                z.get_parent().get_left_child(),
                z.get_parent().get_parent(),
                z.get_parent().get_parent().get_left_child()
            );

            if z.get_parent() == z.get_parent().get_parent().get_left_child() {
                let y = z.get_parent().get_parent().get_right_child();
                if y.is_red() {
                    z.get_parent().set_color(Color::Black);
                    y.set_color(Color::Black);
                    z.get_parent().get_parent().set_color(Color::Red);
                    z = z.get_parent().get_parent();
                } else if z == z.get_parent().get_right_child() {
                    z = z.get_parent();
                    self.left_rotate(z.clone());
                }
                z.get_parent().set_color(Color::Black);
                z.get_parent().get_parent().set_color(Color::Red);
                self.right_rotate(z.get_parent().get_parent().clone())
            } else {
                let y = z.get_parent().get_parent().get_left_child();
                if y.is_red() {
                    z.get_parent().set_color(Color::Black);
                    y.set_color(Color::Black);
                    z.get_parent().get_parent().set_color(Color::Red);
                    z = z.get_parent().get_parent();
                } else if z == z.get_parent().get_left_child() {
                    z = z.get_parent();
                    self.right_rotate(z.clone());
                }
                z.get_parent().set_color(Color::Black);
                z.get_parent().get_parent().set_color(Color::Red);
                self.left_rotate(z.get_parent().get_parent().clone())
            }
        }
        self.root.set_color(Color::Black);
    }

    /// From page 290 of CLRS
    fn rb_search(&self, key: &K) -> NodeLink<K, V> {
        let mut x = self.root.clone();
        while x.is_some() && x.get_key().unwrap() != key {
            if key < x.get_key().unwrap() {
                println!("Traversing left!");
                x = x.get_left_child().clone();
            } else {
                println!("Traversing right!");
                x = x.get_right_child().clone();
            }
        }
        return x;
    }

    pub fn peek(&self) -> Option<&K> {
        if self.root.is_none() {
            return None;
        }

        unsafe {
            return Some(&(*self.root.inner.unwrap().as_ptr()).key);
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        let node = self.rb_search(key);
        if node.is_none() {
            return None;
        }

        unsafe { Some(&(*node.inner.unwrap().as_ptr()).value) }
    }

    pub fn put(&mut self, key: K, value: V) -> () {
        println!("Putting!");
        self.rb_insert(NodeLink::new(key, value))
    }
}

mod test {
    use crate::memtable::{Memtable, Node};

    #[test]
    fn insert_none() {
        let store: Memtable<i32, i32> = Memtable::new(100);
        assert_eq!(store.get(&100), None);
    }

    #[test]
    fn insert_one() {
        let mut store = Memtable::new(100);
        store.put(1, "My favorite value!");
        let back = store.get(&1);
        assert!(back.is_some());
        assert_eq!(back, Some(&"My favorite value!"));
    }

    #[test]
    fn insert_two() {
        let mut store = Memtable::new(100);
        store.put(50, 0);
        store.put(100, 1);
        assert_eq!(store.get(&50), Some(&0));
        assert_eq!(store.get(&100), Some(&1));

        assert_eq!(store.peek(), Some(&50));
    }

    #[test]
    fn insert_two_out_of_order() {
        let mut store = Memtable::new(100);
        store.put(100, 1);
        store.put(50, 0);
        assert_eq!(store.get(&50), Some(&0));
        assert_eq!(store.get(&100), Some(&1));

        unsafe {
            println!("{:#?}", *store.root.inner.unwrap().as_ptr());
        }
        assert_eq!(store.peek(), Some(&100));
    }

    #[test]
    fn insert_many() {
        let mut store = crate::memtable::Memtable::new(100);
        println!("1");
        store.put(0, String::from("zero"));
        assert_eq!(store.get(&0), Some(&String::from("zero")));
        assert_eq!(store.peek(), Some(&0));

        println!("2");
        store.put(1, String::from("one"));
        assert_eq!(store.get(&1), Some(&String::from("one")));
        assert_eq!(store.peek(), Some(&0));

        println!("3");
        store.put(2, String::from("two"));
        assert_eq!(store.get(&2), Some(&String::from("two")));
        assert_eq!(store.peek(), Some(&1));
    }
}
