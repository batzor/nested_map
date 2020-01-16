use crossbeam_epoch::pin;
use nested_map::nested_map::NestedMap;
use rayon::prelude::*;

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
fn insert_same_bucket() {
    let map = NestedMap::default();
    assert!(map.insert(91, 1, &pin()).is_none());
    assert!(map.insert(256, 2, &pin()).is_none());
    assert_eq!(map.lookup(&91, &pin()), Some(&1));
    assert_eq!(map.lookup(&256, &pin()), Some(&2));
    assert!(map.delete(&91, &pin()).is_ok());
    assert_eq!(map.lookup(&91, &pin()), None);
    assert_eq!(map.lookup(&256, &pin()), Some(&2));
}

#[test]
fn insert_many() {
    let map = NestedMap::default();

    for i in 0..100000 {
        assert!(map.insert(i, i * 5, &pin()).is_none());
    }
    for i in 0..100000 {
        assert_eq!(map.lookup(&i, &pin()), Some(&(i * 5)));
    }
}

#[test]
fn insert_rayon() {
    let map = NestedMap::default();

    (0..10000).into_par_iter().for_each(|i| {
        assert!(map.insert(i, i * 5, &pin()).is_none());
    });
    (0..10000).into_par_iter().for_each(|i| {
        assert_eq!(map.lookup(&i, &pin()), Some(&(i * 5)));
    });
}
