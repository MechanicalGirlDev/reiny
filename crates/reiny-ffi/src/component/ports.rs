use super::{
    Bytes, CLOSED, COMPLETED, Endpoint, Error, FAILURE, INVALID, Mutex, NO_REPLY, Object, REMOTE,
    Reservation, SCHEMA, TIMEOUT, abi, boundary, budget, export, failure, insert, lock, object,
    ordinary, runtime, variant, wait, watch, wrong,
};
use abi::{copy_out, output};

fn call_error(error: &reiny::CallError) -> Error {
    let code = match error {
        reiny::CallError::NoReply => NO_REPLY,
        reiny::CallError::Timeout => TIMEOUT,
        reiny::CallError::Remote(_) => REMOTE,
        reiny::CallError::Schema { .. } | reiny::CallError::MissingSchema => SCHEMA,
        reiny::CallError::Decode(_) | reiny::CallError::Engine(_) => FAILURE,
    };
    Error::new(code, error.to_string())
}

export!(reiny_exec_context_open_stream(handle: u64, name: Bytes, schema: u64, direction: u32, out: *mut u64) {
    output(out)?;
    let value = object(handle)?;
    let context = variant!(value, Context).clone();
    let schema_object = object(schema)?;
    let schema = variant!(schema_object, Schema);
    let name = name.text()?;
    let reservation = Reservation::new()?;
    context.admit(true)?;
    let endpoint = match direction {
        1 => Object::Subscriber(Endpoint { inner: tokio::sync::Mutex::new(context.inner.raw_input(&name, schema).map_err(failure)?), context }),
        2 => Object::Publisher(Endpoint { inner: context.inner.raw_output(&name, schema).map_err(failure)?, context }),
        _ => return Err(Error::new(INVALID, "stream direction must be input=1 or output=2")),
    };
    out.write(reservation.publish(endpoint)?);
    Ok(())
});

export!(reiny_exec_context_open_rpc(handle: u64, name: Bytes, request: u64, response: u64, direction: u32, out: *mut u64) {
    output(out)?;
    let value = object(handle)?;
    let context = variant!(value, Context).clone();
    let request_object = object(request)?;
    let response_object = object(response)?;
    let request = variant!(request_object, Schema);
    let response = variant!(response_object, Schema);
    let name = name.text()?;
    let reservation = Reservation::new()?;
    context.admit(true)?;
    let endpoint = match direction {
        1 => Object::Client(Endpoint { inner: context.inner.raw_rpc_client(&name, request, response).map_err(failure)?, context }),
        2 => Object::Server(Endpoint { inner: tokio::sync::Mutex::new(context.inner.raw_rpc_server(&name, request, response).map_err(failure)?), context }),
        _ => return Err(Error::new(INVALID, "RPC direction must be client=1 or server=2")),
    };
    out.write(reservation.publish(endpoint)?);
    Ok(())
});

export!(reiny_exec_publisher_send(handle: u64, payload: Bytes) {
    let bytes = payload.bytes()?;
    let value = object(handle)?;
    let publisher = variant!(value, Publisher);
    publisher.context.admit(false)?;
    publisher.inner.send(bytes).map_err(failure)
});

export!(reiny_exec_subscription_receive(handle: u64, timeout_ns: u64, out: *mut u64) {
    output(out)?;
    let value = object(handle)?;
    let subscriber = variant!(value, Subscriber);
    let reservation = Reservation::new()?;
    subscriber.context.admit(false)?;
    let message = wait(timeout_ns, async {
        let mut subscriber_inner = subscriber.inner.lock().await;
        tokio::select! {
            biased;
            () = subscriber.context.stopped() => Err(Error::new(CLOSED, "component stopped")),
            message = subscriber_inner.recv() => message.ok_or_else(|| Error::new(CLOSED, "stream closed")),
        }
    })?;
    out.write(reservation.publish(Object::Message(message.into()))?);
    Ok(())
});

export!(reiny_exec_subscription_stats(handle: u64, timeout_ns: u64, out: *mut u64) {
    output(out)?;
    let value = object(handle)?;
    let subscriber = variant!(value, Subscriber);
    let stats = wait(timeout_ns, async { Ok(subscriber.inner.lock().await.stats()) })?;
    let json = serde_json::json!({"received": stats.received, "dropped": stats.dropped, "blocked": stats.blocked});
    out.write(insert(Object::Buffer(serde_json::to_vec(&json).map_err(failure)?), false)?);
    Ok(())
});

export!(reiny_exec_message_copy(handle: u64, field: u32, dest: *mut u8, capacity: usize, required: *mut usize) {
    let value = object(handle)?;
    let message = variant!(value, Message);
    let bytes = match field {
        0 => message.payload.clone(),
        1 => { let mut bytes = message.source.as_bytes().to_vec(); bytes.push(0); bytes },
        2 => { let mut bytes = message.source_port.as_bytes().to_vec(); bytes.push(0); bytes },
        3 => serde_json::to_vec(&serde_json::json!({"schema": message.schema, "timestamp": message.timestamp})).map_err(failure)?,
        _ => return Err(Error::new(INVALID, "unknown message field")),
    };
    copy_out(&bytes, dest, capacity, required)
});

export!(reiny_exec_rpc_call_start(handle: u64, payload: Bytes, timeout_ns: u64, out: *mut u64) {
    output(out)?;
    let payload = payload.bytes()?;
    let timeout = budget(timeout_ns)?;
    let value = object(handle)?;
    let client = variant!(value, Client);
    let reservation = Reservation::new()?;
    let runtime = runtime()?;
    client.context.admit(false)?;
    let (sender, receiver) = watch::channel(None);
    let id = reservation.publish(Object::Operation(receiver))?;
    runtime.spawn(async move {
        let Object::Client(client) = value.as_ref() else { return; };
        let result = tokio::select! {
            biased;
            () = sender.closed() => return,
            () = client.context.stopped() => Err(Error::new(CLOSED, "component stopped")),
            result = client.inner.call_with_timeout(payload, timeout) => result.map_err(|error| call_error(&error)),
        };
        sender.send_replace(Some(result.map_err(|error| (error.code, error.message))));
    });
    out.write(id);
    Ok(())
});

export!(reiny_exec_operation_wait(handle: u64, timeout_ns: u64, out: *mut u64) {
    output(out)?;
    let value = object(handle)?;
    let mut operation = variant!(value, Operation).clone();
    let reservation = Reservation::new()?;
    let bytes = wait(timeout_ns, async {
        loop {
            if let Some(result) = operation.borrow_and_update().clone() {
                return result.map_err(|(code, message)| Error::new(code, message));
            }
            operation.changed().await.map_err(|_| Error::new(CLOSED, "operation worker terminated"))?;
        }
    })?;
    out.write(reservation.publish(Object::Buffer(bytes))?);
    Ok(())
});

export!(reiny_exec_rpc_server_receive(handle: u64, timeout_ns: u64, out: *mut u64) {
    output(out)?;
    let value = object(handle)?;
    let server = variant!(value, Server);
    let reservation = Reservation::new()?;
    server.context.admit(false)?;
    let request = wait(timeout_ns, async {
        let mut inner = server.inner.lock().await;
        tokio::select! {
            biased;
            () = server.context.stopped() => Err(Error::new(CLOSED, "component stopped")),
            request = inner.recv() => request.ok_or_else(|| Error::new(CLOSED, "RPC server closed")),
        }
    })?;
    out.write(reservation.publish(Object::Request(Endpoint { context: server.context.clone(), inner: Mutex::new(Some(request)) }))?);
    Ok(())
});

export!(reiny_exec_request_copy(handle: u64, dest: *mut u8, capacity: usize, required: *mut usize) {
    let value = object(handle)?;
    let request = lock(&variant!(value, Request).inner)?;
    let request = request.as_ref().ok_or_else(|| Error::new(COMPLETED, "request already replied"))?;
    copy_out(request.payload(), dest, capacity, required)
});

export!(reiny_exec_request_reply(handle: u64, payload: Bytes, error_reply: u32) {
    if error_reply > 1 { return Err(Error::new(INVALID, "invalid reply kind")); }
    let payload = if error_reply == 1 { payload.text()?.into_bytes() } else { payload.bytes()? };
    ordinary()?;
    let runtime = runtime()?;
    let value = object(handle)?;
    let request = variant!(value, Request);
    request.context.admit(false)?;
    let request = lock(&request.inner)?.take().ok_or_else(|| Error::new(COMPLETED, "request already replied"))?;
    if error_reply == 1 {
        runtime.block_on(request.reply_err(String::from_utf8(payload).map_err(failure)?)).map_err(failure)
    } else {
        runtime.block_on(request.reply(payload)).map_err(failure)
    }
});

export!(reiny_exec_context_watch_input_ready(handle: u64, name: Bytes, out: *mut u64) {
    output(out)?;
    let name = name.text()?;
    let value = object(handle)?;
    let context = variant!(value, Context).clone();
    let reservation = Reservation::new()?;
    context.admit(true)?;
    let presence = context.inner.watch_input_ready(&name).map_err(failure)?;
    out.write(reservation.publish(Object::Presence(Endpoint { context, inner: tokio::sync::Mutex::new(presence) }))?);
    Ok(())
});

export!(reiny_exec_presence_next(handle: u64, timeout_ns: u64, out: *mut u64) {
    output(out)?;
    let value = object(handle)?;
    let presence = variant!(value, Presence);
    let reservation = Reservation::new()?;
    presence.context.admit(false)?;
    let event = wait(timeout_ns, async {
        let mut inner = presence.inner.lock().await;
        tokio::select! {
            biased;
            () = presence.context.stopped() => Err(Error::new(CLOSED, "component stopped")),
            event = inner.recv() => event.ok_or_else(|| Error::new(CLOSED, "readiness observer closed")),
        }
    })?;
    let (joined, key) = match event {
        reiny::engine::Presence::Joined(key) => (true, key),
        reiny::engine::Presence::Left(key) => (false, key),
    };
    let bytes = serde_json::to_vec(&serde_json::json!({"joined": joined, "key": key.to_string()})).map_err(failure)?;
    out.write(reservation.publish(Object::Buffer(bytes))?);
    Ok(())
});
