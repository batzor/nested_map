use rayon::prelude::*;
use nested_map::nested_map::NestedMap;
use crossbeam_epoch::pin;

#[test]
fn smoke() {
    let map = NestedMap::default();
    assert!(map.insert("aa", 42, &pin()).is_none());
    assert!(map.insert("bb", 58, &pin()).is_none());
    assert_eq!(map.insert("aa", 37, &pin()), Some(42));
    assert_eq!(map.lookup(&"aa", &pin()), Some(&37));
    assert_eq!(map.lookup(&"bb", &pin()), Some(&58));
    assert!(map.delete(&"aa", &pin()).is_ok());
    assert_eq!(map.lookup(&"aa", &pin()), None);
}

#[test]
fn insert_rayon() {
    const INSV: u64 = 518;
    let map = NestedMap::default();

    (0..10000).into_par_iter().for_each(|i| {
        assert!(map.insert(i, i * 5, &pin()).is_none());
    });
    (0..10000).into_par_iter().for_each(|i| {
        assert_eq!(map.lookup(&i, &pin()), Some(&(i * 5)));
    });
}
