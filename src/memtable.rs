struct Memtable<V> {}

enum PutError {}

impl Memtable {
    pub fn new(memtable_size: i32) -> self {
        Memtable {}
    }

    pub async fn get(&self, key: Into<String>) -> Option<V> {
        todo!()
    }

    pub async fn put(&mut self, key: Into<String>, value: V) -> Result<(), PutError> {
        todo!()
    }
}
