pub mod memtable;

use memtable::Memtable;

fn main() {
    let mut store = Memtable::new(100);
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

    println!("4");
}
