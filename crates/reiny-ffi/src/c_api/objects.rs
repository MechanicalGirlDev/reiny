//! One monotonic handle table shared by transport and executable objects.

use super::{LocalBus, Message, Publisher, Result, Session, Subscription};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
};

#[derive(Clone)]
pub(crate) enum Object {
    Bus(Arc<LocalBus>),
    Session(Arc<Session>),
    Publisher(Arc<Publisher>),
    Subscription(Arc<Subscription>),
    Message(Arc<Message>),
    Buffer(Arc<Vec<u8>>),
    Executable(crate::component::Stored),
    Reserved,
}

struct Registry {
    next: u64,
    objects: HashMap<u64, Object>,
}

fn registry() -> &'static Mutex<Registry> {
    static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        Mutex::new(Registry {
            next: 1,
            objects: HashMap::new(),
        })
    })
}

pub(crate) fn insert(object: Object) -> Result<u64> {
    let mut registry = registry().lock().map_err(|_| "handle registry poisoned")?;
    let id = registry.next;
    registry.next = id.checked_add(1).ok_or("handle space exhausted")?;
    registry.objects.insert(id, object);
    Ok(id)
}

pub(crate) fn get(id: u64) -> Result<Object> {
    registry()
        .lock()
        .map_err(|_| "handle registry poisoned")?
        .objects
        .get(&id)
        .cloned()
        .ok_or_else(|| "invalid or released handle".into())
}

pub(crate) fn replace_reserved(id: u64, value: Object) -> Result<()> {
    let mut registry = registry().lock().map_err(|_| "handle registry poisoned")?;
    let slot = registry.objects.get_mut(&id).ok_or("reservation lost")?;
    if !matches!(slot, Object::Reserved) {
        return Err("handle is not reserved".into());
    }
    *slot = value;
    Ok(())
}

pub(crate) fn remove(id: u64, borrowed: bool) -> Result<Object> {
    let mut registry = registry().lock().map_err(|_| "handle registry poisoned")?;
    if !borrowed
        && matches!(registry.objects.get(&id), Some(Object::Executable(value)) if value.borrowed)
    {
        return Err("borrowed callback handle cannot be released".into());
    }
    registry
        .objects
        .remove(&id)
        .ok_or_else(|| "invalid or released handle".into())
}
