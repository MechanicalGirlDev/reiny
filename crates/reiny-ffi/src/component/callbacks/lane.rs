use super::{
    Arc, CALLBACK, CLOSED, Command, Context, Error, Factory, INVALID, Mutex, Object, Result,
    failure, insert, lock, mpsc, object, variant, wrong,
};
use std::ffi::c_void;

pub(super) struct Lane {
    pub(super) factory: Arc<Factory>,
    pub(super) context: Arc<Context>,
    pub(super) id: u64,
    pub(super) instance: *mut c_void,
}

impl Lane {
    pub(super) fn invoke(call: impl FnOnce(u64) -> i32) -> Result<()> {
        let sink = insert(Object::ErrorSink(Mutex::new(None)), true)?;
        let code = call(sink);
        let value = object(sink)?;
        let diagnostic = lock(variant!(value, ErrorSink))?.take();
        drop(crate::c_api::remove(sink, true).map_err(failure)?);
        if code == 0 {
            return Ok(());
        }
        Err(diagnostic
            .unwrap_or_else(|| Error::new(CALLBACK, format!("foreign callback returned {code}"))))
    }

    fn prepare(&mut self) -> Result<()> {
        let mut instance = std::ptr::null_mut();
        let table = self.factory.table;
        let create = table
            .create
            .ok_or_else(|| Error::new(INVALID, "missing create"))?;
        Self::invoke(|sink| {
            // SAFETY: [FFI/lifetime] table was validated at registration, factory
            // remains owned by this lane, and instance is a writable local slot.
            unsafe { create(table.factory_data, self.id, &raw mut instance, sink) }
        })?;
        if instance.is_null() {
            return Err(Error::new(CALLBACK, "create succeeded without an instance"));
        }
        self.instance = instance;
        let prepare = table
            .prepare
            .ok_or_else(|| Error::new(INVALID, "missing prepare"))?;
        Self::invoke(|sink| {
            // SAFETY: [FFI/lifetime] successful create transferred this instance;
            // only this lane calls instance hooks and callbacks cannot unwind.
            unsafe { prepare(self.instance, self.id, sink) }
        })
    }

    pub(super) fn serve(&mut self, receiver: &mpsc::Receiver<Command>) {
        let mut created = false;
        let mut stopped = false;
        while let Ok(command) = receiver.recv() {
            let (result, reply) = match command {
                Command::Step(tick, inputs, reply) => {
                    let result = self.step(tick, inputs);
                    if reply.send(result).is_err() {
                        tracing::debug!("step observer cancelled");
                    }
                    continue;
                }
                Command::Prepare(reply) => {
                    let result = self.prepare();
                    created = !self.instance.is_null();
                    (result, reply)
                }
                Command::Run(reply) => {
                    let result = (|| {
                        {
                            let mut phase = lock(&self.context.phase)?;
                            if *phase == 2 {
                                return Err(Error::new(CLOSED, "component stopped before run"));
                            }
                            *phase = 1;
                        }
                        let run = self
                            .factory
                            .table
                            .run
                            .ok_or_else(|| Error::new(INVALID, "missing run"))?;
                        Self::invoke(|sink| {
                            // SAFETY: [FFI/lifetime] same lane owns the created
                            // instance; stop/destroy cannot run until this returns.
                            unsafe { run(self.instance, self.id, sink) }
                        })
                    })();
                    (result, reply)
                }
                Command::Stop(reason, reply) => {
                    let result = if created { self.stop(reason) } else { Ok(()) };
                    stopped = true;
                    self.destroy(created);
                    if reply.send(result).is_err() {
                        tracing::debug!("stop observer cancelled");
                    }
                    break;
                }
            };
            if reply.send(result).is_err() {
                tracing::debug!("callback observer cancelled");
            }
        }
        if !stopped {
            self.context.close();
            if created && let Err(error) = self.stop(1) {
                tracing::error!(%error, "detached foreign stop failed");
            }
            self.destroy(created);
        }
    }

    fn stop(&self, reason: u32) -> Result<()> {
        let stop = self
            .factory
            .table
            .stop
            .ok_or_else(|| Error::new(INVALID, "missing stop"))?;
        Self::invoke(|sink| {
            // SAFETY: [FFI/lifetime] this call follows the active callback on the
            // same thread; the instance is retained until the following destroy.
            unsafe { stop(self.instance, self.id, reason, sink) }
        })
    }

    fn destroy(&mut self, created: bool) {
        if created && let Some(destroy) = self.factory.table.destroy_instance {
            // SAFETY: [double-free/lifetime] serve calls destroy exactly once
            // after all hooks return, never concurrently with another callback.
            unsafe {
                destroy(self.instance);
            }
            self.instance = std::ptr::null_mut();
        }
    }
}

impl Drop for Lane {
    fn drop(&mut self) {
        self.context.close();
        if let Err(error) = crate::c_api::remove(self.id, true) {
            tracing::error!(%error, "cannot release borrowed context");
        }
    }
}
