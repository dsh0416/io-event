use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::ffi::c_void;
#[cfg(unix)]
use std::os::raw::c_int;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(unix)]
use std::sync::atomic::AtomicI32;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use magnus::gc;
use magnus::value::{Opaque, ReprValue};
use magnus::{
    function, method, prelude::*, DataTypeFunctions, Error, IntoValue, RHash, Ruby, Symbol,
    TryConvert, TypedData, Value,
};

const READABLE: i32 = 1;
const PRIORITY: i32 = 2;
const WRITABLE: i32 = 4;

#[cfg(unix)]
const RB_NOGVL_OFFLOAD_SAFE: u32 = 0x4;

#[cfg(unix)]
type RawDescriptor = c_int;

#[cfg(windows)]
type RawDescriptor = i64;

#[cfg(unix)]
extern "C" {
    fn rb_io_buffer_get_bytes_for_writing(
        buffer: rb_sys::VALUE,
        base: *mut *mut c_void,
        size: *mut usize,
    );
    fn rb_io_buffer_get_bytes_for_reading(
        buffer: rb_sys::VALUE,
        base: *mut *const c_void,
        size: *mut usize,
    );
}

#[cfg(unix)]
extern "C" {
    fn rb_thread_call_without_gvl(
        func: unsafe extern "C" fn(*mut c_void) -> *mut c_void,
        data1: *mut c_void,
        ubf: *mut c_void,
        data2: *mut c_void,
    ) -> *mut c_void;
}

#[derive(Clone, Copy)]
enum BackendKind {
    EPoll,
    KQueue,
    IOCP,
    #[cfg(all(target_os = "linux", feature = "uring"))]
    URing,
}

#[derive(Clone)]
struct ReadyEntry {
    id: usize,
    fiber: Opaque<Value>,
}

struct WaiterState {
    ready: i32,
    cancelled: bool,
}

struct IoWaiter {
    token: usize,
    fd: RawDescriptor,
    io: Opaque<Value>,
    fiber: Opaque<Value>,
    events: i32,
    state: Arc<Mutex<WaiterState>>,
}

struct ProcessWaiter {
    token: usize,
    pid: i64,
    flags: i32,
    fiber: Opaque<Value>,
    result: Arc<Mutex<Option<Opaque<Value>>>>,
}

struct SelectorCore {
    kind: BackendKind,
    loop_fiber: Option<Opaque<Value>>,
    ready: VecDeque<ReadyEntry>,
    io_waiters: Vec<IoWaiter>,
    process_waiters: Vec<ProcessWaiter>,
    next_token: usize,
    idle_duration: f64,
    closed: bool,
    blocked: bool,
    #[cfg(unix)]
    interrupt: Option<Interrupt>,
}

impl SelectorCore {
    fn new(kind: BackendKind) -> Self {
        Self {
            kind,
            loop_fiber: None,
            ready: VecDeque::new(),
            io_waiters: Vec::new(),
            process_waiters: Vec::new(),
            next_token: 1,
            idle_duration: 0.0,
            closed: false,
            blocked: false,
            #[cfg(unix)]
            interrupt: None,
        }
    }

    fn initialize(&mut self, loop_fiber: Value) -> Result<(), Error> {
        self.loop_fiber = Some(loop_fiber.into());
        self.closed = false;

        #[cfg(unix)]
        {
            if self.interrupt.is_none() {
                self.interrupt = Some(Interrupt::open()?);
            }
        }

        Ok(())
    }

    fn next_token(&mut self) -> usize {
        let token = self.next_token;
        self.next_token = self.next_token.wrapping_add(1).max(1);
        token
    }

    fn mark(&self, marker: &gc::Marker) {
        if let Some(loop_fiber) = self.loop_fiber {
            marker.mark(loop_fiber);
        }

        for entry in &self.ready {
            marker.mark(entry.fiber);
        }

        for waiter in &self.io_waiters {
            marker.mark(waiter.io);
            marker.mark(waiter.fiber);
        }

        for waiter in &self.process_waiters {
            marker.mark(waiter.fiber);
            if let Some(result) = *waiter.result.lock().unwrap() {
                marker.mark(result);
            }
        }
    }

    fn close(&mut self) {
        self.closed = true;
        self.loop_fiber = None;
        self.ready.clear();
        self.io_waiters.clear();
        self.process_waiters.clear();

        #[cfg(unix)]
        {
            self.interrupt = None;
        }
    }

    fn loop_value(&self, ruby: &Ruby) -> Value {
        self.loop_fiber
            .map(|value| ruby.get_inner(value))
            .unwrap_or_else(|| ruby.qnil().as_value())
    }

    fn push_ready(&mut self, fiber: Value) -> usize {
        let id = self.next_token();
        self.ready.push_back(ReadyEntry {
            id,
            fiber: fiber.into(),
        });
        id
    }

    fn remove_ready(&mut self, id: usize) {
        if let Some(index) = self.ready.iter().position(|entry| entry.id == id) {
            self.ready.remove(index);
        }
    }

    fn remove_io_waiter(&mut self, token: usize) {
        if let Some(index) = self.io_waiters.iter().position(|waiter| waiter.token == token) {
            self.io_waiters.swap_remove(index);
        }
    }

    fn remove_process_waiter(&mut self, token: usize) {
        if let Some(index) = self
            .process_waiters
            .iter()
            .position(|waiter| waiter.token == token)
        {
            self.process_waiters.swap_remove(index);
        }
    }
}

impl Drop for SelectorCore {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(unix)]
struct Interrupt {
    input: RawDescriptor,
    output: RawDescriptor,
}

#[cfg(unix)]
impl Interrupt {
    fn open() -> Result<Self, Error> {
        let mut fds = [0; 2];
        let result = unsafe { libc::pipe(fds.as_mut_ptr()) };
        if result == -1 {
            return Err(errno_error("IO_Event_Interrupt_open:pipe"));
        }

        set_nonblock(fds[0])?;
        set_nonblock(fds[1])?;

        Ok(Self {
            input: fds[0],
            output: fds[1],
        })
    }

    fn signal(&self) -> Result<(), Error> {
        let byte = [b'.'];
        let result = unsafe { libc::write(self.output, byte.as_ptr().cast(), 1) };
        if result == -1 {
            let errno = errno();
            if errno == libc::EAGAIN || errno == libc::EWOULDBLOCK {
                return Ok(());
            }

            return Err(errno_error("IO_Event_Interrupt_signal:write"));
        }

        Ok(())
    }

    fn clear(&self) -> Result<(), Error> {
        let mut buffer = [0u8; 128];
        loop {
            let result = unsafe {
                libc::read(
                    self.input,
                    buffer.as_mut_ptr().cast::<c_void>(),
                    buffer.len(),
                )
            };

            if result > 0 {
                continue;
            }

            if result == -1 {
                let errno = errno();
                if errno == libc::EAGAIN || errno == libc::EWOULDBLOCK {
                    return Ok(());
                }

                return Err(errno_error("IO_Event_Interrupt_clear:read"));
            }

            return Ok(());
        }
    }
}

#[cfg(unix)]
impl Drop for Interrupt {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.input);
            libc::close(self.output);
        }
    }
}

#[derive(TypedData)]
#[magnus(class = "IO::Event::Selector::EPoll", free_immediately, mark, size)]
struct EPollSelector {
    core: RefCell<SelectorCore>,
}

#[derive(TypedData)]
#[magnus(class = "IO::Event::Selector::KQueue", free_immediately, mark, size)]
struct KQueueSelector {
    core: RefCell<SelectorCore>,
}

#[derive(TypedData)]
#[magnus(class = "IO::Event::Selector::IOCP", free_immediately, mark, size)]
struct IOCPSelector {
    core: RefCell<SelectorCore>,
}

#[cfg(all(target_os = "linux", feature = "uring"))]
#[derive(TypedData)]
#[magnus(class = "IO::Event::Selector::URing", free_immediately, mark, size)]
struct URingSelector {
    core: RefCell<SelectorCore>,
}

macro_rules! selector_impl {
    ($type:ty, $kind:expr) => {
        impl Default for $type {
            fn default() -> Self {
                Self {
                    core: RefCell::new(SelectorCore::new($kind)),
                }
            }
        }

        impl DataTypeFunctions for $type {
            fn mark(&self, marker: &gc::Marker) {
                if let Ok(core) = self.core.try_borrow() {
                    core.mark(marker);
                }
            }
        }

        impl $type {
            fn initialize(&self, loop_fiber: Value) -> Result<Value, Error> {
                self.core.borrow_mut().initialize(loop_fiber)?;
                Ok(loop_fiber)
            }

            fn loop_fiber(&self) -> Value {
                let ruby = Ruby::get().unwrap();
                self.core.borrow().loop_value(&ruby)
            }

            fn idle_duration(&self) -> f64 {
                self.core.borrow().idle_duration
            }

            fn transfer(&self) -> Result<Value, Error> {
                let ruby = Ruby::get().unwrap();
                selector_transfer(&ruby, &self.core)
            }

            fn resume(&self, args: &[Value]) -> Result<Value, Error> {
                let ruby = Ruby::get().unwrap();
                selector_resume(&ruby, &self.core, args)
            }

            fn yield_fiber(&self) -> Result<Value, Error> {
                let ruby = Ruby::get().unwrap();
                selector_yield(&ruby, &self.core)
            }

            fn push(&self, fiber: Value) -> Value {
                self.core.borrow_mut().push_ready(fiber);
                Ruby::get_with(fiber).qnil().as_value()
            }

            fn raise(&self, args: &[Value]) -> Result<Value, Error> {
                let ruby = Ruby::get().unwrap();
                selector_raise(&ruby, &self.core, args)
            }

            fn ready_p(&self) -> bool {
                !self.core.borrow().ready.is_empty()
            }

            fn select(&self, duration: Value) -> Result<i64, Error> {
                let ruby = Ruby::get().unwrap();
                selector_select(&ruby, &self.core, duration)
            }

            fn wakeup(&self) -> Result<bool, Error> {
                selector_wakeup(&self.core)
            }

            fn close(&self) -> Value {
                let ruby = Ruby::get().unwrap();
                self.core.borrow_mut().close();
                ruby.qnil().as_value()
            }

            fn io_wait(
                &self,
                fiber: Value,
                io: Value,
                events: i32,
            ) -> Result<Value, Error> {
                let ruby = Ruby::get().unwrap();
                selector_io_wait(&ruby, &self.core, fiber, io, events)
            }

            fn io_read(&self, args: &[Value]) -> Result<Value, Error> {
                let ruby = Ruby::get().unwrap();
                selector_io_read(&ruby, &self.core, args)
            }

            fn io_write(&self, args: &[Value]) -> Result<Value, Error> {
                let ruby = Ruby::get().unwrap();
                selector_io_write(&ruby, &self.core, args)
            }

            fn process_wait(
                &self,
                fiber: Value,
                pid: i64,
                flags: i32,
            ) -> Result<Value, Error> {
                let ruby = Ruby::get().unwrap();
                selector_process_wait(&ruby, &self.core, fiber, pid, flags)
            }
        }
    };
}

selector_impl!(EPollSelector, BackendKind::EPoll);
selector_impl!(KQueueSelector, BackendKind::KQueue);
selector_impl!(IOCPSelector, BackendKind::IOCP);
#[cfg(all(target_os = "linux", feature = "uring"))]
selector_impl!(URingSelector, BackendKind::URing);

#[cfg(all(target_os = "linux", feature = "uring"))]
impl URingSelector {
    fn io_pread(&self, args: &[Value]) -> Result<Value, Error> {
        let ruby = Ruby::get().unwrap();
        selector_io_pread(&ruby, &self.core, args)
    }

    fn io_pwrite(&self, args: &[Value]) -> Result<Value, Error> {
        let ruby = Ruby::get().unwrap();
        selector_io_pwrite(&ruby, &self.core, args)
    }

    fn io_close(&self, descriptor: i32) -> bool {
        #[cfg(unix)]
        unsafe {
            libc::close(descriptor);
        }

        true
    }
}

fn selector_transfer(ruby: &Ruby, core: &RefCell<SelectorCore>) -> Result<Value, Error> {
    let loop_fiber = core.borrow().loop_value(ruby);
    if same_value(loop_fiber, ruby.fiber_current().as_value()) {
        return Ok(ruby.qnil().as_value());
    }

    transfer_value(loop_fiber, &[])
}

fn selector_resume(
    ruby: &Ruby,
    core: &RefCell<SelectorCore>,
    args: &[Value],
) -> Result<Value, Error> {
    if args.is_empty() {
        return Err(Error::new(
            ruby.exception_arg_error(),
            "wrong number of arguments (given 0, expected 1+)",
        ));
    }

    let current = ruby.fiber_current().as_value();
    let ready_id = core.borrow_mut().push_ready(current);
    let result = transfer_value(args[0], &args[1..]);
    core.borrow_mut().remove_ready(ready_id);
    result
}

fn selector_yield(ruby: &Ruby, core: &RefCell<SelectorCore>) -> Result<Value, Error> {
    let loop_fiber = core.borrow().loop_value(ruby);
    selector_resume(ruby, core, &[loop_fiber])
}

fn selector_raise(
    ruby: &Ruby,
    core: &RefCell<SelectorCore>,
    args: &[Value],
) -> Result<Value, Error> {
    if args.len() < 2 {
        return Err(Error::new(
            ruby.exception_arg_error(),
            "wrong number of arguments (given 0..1, expected 2+)",
        ));
    }

    let current = ruby.fiber_current().as_value();
    let ready_id = core.borrow_mut().push_ready(current);
    let result = args[0].funcall("raise", &args[1..]);
    core.borrow_mut().remove_ready(ready_id);
    result
}

fn selector_io_wait(
    ruby: &Ruby,
    core: &RefCell<SelectorCore>,
    fiber: Value,
    io: Value,
    events: i32,
) -> Result<Value, Error> {
    let fd = io_descriptor(io)?;
    let state = Arc::new(Mutex::new(WaiterState {
        ready: 0,
        cancelled: false,
    }));
    let token = {
        let mut core = core.borrow_mut();
        let token = core.next_token();
        core.io_waiters.push(IoWaiter {
            token,
            fd,
            io: io.into(),
            fiber: fiber.into(),
            events,
            state: state.clone(),
        });
        token
    };

    let result = selector_transfer(ruby, core);
    core.borrow_mut().remove_io_waiter(token);
    let ready = {
        let mut state = state.lock().unwrap();
        state.cancelled = true;
        state.ready
    };
    result?;

    if ready == 0 {
        Ok(ruby.qfalse().as_value())
    } else {
        Ok(ready.into_value_with(ruby))
    }
}

fn selector_process_wait(
    ruby: &Ruby,
    core: &RefCell<SelectorCore>,
    fiber: Value,
    pid: i64,
    flags: i32,
) -> Result<Value, Error> {
    let status = process_status_wait(ruby, pid, flags)?;
    if !status.is_nil() {
        return Ok(status);
    }

    let result = Arc::new(Mutex::new(None));
    let token = {
        let mut core = core.borrow_mut();
        let token = core.next_token();
        core.process_waiters.push(ProcessWaiter {
            token,
            pid,
            flags,
            fiber: fiber.into(),
            result: result.clone(),
        });
        token
    };

    let transfer_result = selector_transfer(ruby, core);
    core.borrow_mut().remove_process_waiter(token);
    transfer_result?;

    let status = *result.lock().unwrap();
    if let Some(status) = status {
        Ok(ruby.get_inner(status))
    } else {
        Ok(ruby.qfalse().as_value())
    }
}

fn selector_select(
    ruby: &Ruby,
    core: &RefCell<SelectorCore>,
    duration: Value,
) -> Result<i64, Error> {
    {
        let mut core_ref = core.borrow_mut();
        core_ref.idle_duration = 0.0;
    }

    let ready_count = flush_ready(ruby, core)?;
    let process_ready = check_process_waiters(ruby, core)?;
    if process_ready > 0 {
        return Ok(process_ready as i64);
    }

    let timeout = if ready_count > 0 || !core.borrow().ready.is_empty() {
        Some(0)
    } else {
        duration_to_timeout_ms(duration)?
    };

    let started = if timeout.unwrap_or(0) > 0 || timeout.is_none() {
        Some(Instant::now())
    } else {
        None
    };

    let events = platform_poll(ruby, core, timeout)?;

    if let Some(started) = started {
        core.borrow_mut().idle_duration = started.elapsed().as_secs_f64();
    }

    let mut resumed = dispatch_io_events(ruby, core, events)?;
    resumed += check_process_waiters(ruby, core)?;

    Ok(resumed as i64)
}

fn selector_wakeup(core: &RefCell<SelectorCore>) -> Result<bool, Error> {
    let core_ref = core.borrow();
    if !core_ref.blocked {
        return Ok(false);
    }

    #[cfg(unix)]
    {
        if let Some(interrupt) = &core_ref.interrupt {
            interrupt.signal()?;
            return Ok(true);
        }
    }

    Ok(false)
}

fn flush_ready(ruby: &Ruby, core: &RefCell<SelectorCore>) -> Result<usize, Error> {
    let mut entries = Vec::new();
    {
        let mut core_ref = core.borrow_mut();
        let count = core_ref.ready.len();
        for _ in 0..count {
            if let Some(entry) = core_ref.ready.pop_front() {
                entries.push(entry);
            }
        }
    }

    let mut transferred = 0;
    for entry in entries {
        let fiber = ruby.get_inner(entry.fiber);
        if alive_p(fiber)? {
            transfer_value(fiber, &[])?;
            transferred += 1;
        }
    }

    Ok(transferred)
}

fn dispatch_io_events(
    ruby: &Ruby,
    core: &RefCell<SelectorCore>,
    events: Vec<(RawDescriptor, i32)>,
) -> Result<usize, Error> {
    if events.is_empty() {
        return Ok(0);
    }

    let mut by_fd = HashMap::new();
    for (fd, event) in events {
        by_fd
            .entry(fd)
            .and_modify(|existing| *existing |= event)
            .or_insert(event);
    }

    let mut ready = Vec::new();
    {
        let mut core_ref = core.borrow_mut();
        let mut index = 0;
        while index < core_ref.io_waiters.len() {
            let waiter = &core_ref.io_waiters[index];
            let events = by_fd.get(&waiter.fd).copied().unwrap_or(0) & waiter.events;

            if events == 0 {
                index += 1;
                continue;
            }

            let waiter = core_ref.io_waiters.remove(index);
            waiter.state.lock().unwrap().ready = events;
            ready.push((waiter.fiber, waiter.state));
        }
    }

    let mut count = 0;
    for (fiber, state) in ready {
        if state.lock().unwrap().cancelled {
            continue;
        }

        let fiber = ruby.get_inner(fiber);
        if alive_p(fiber)? {
            transfer_value(fiber, &[])?;
            count += 1;
        }
    }

    Ok(count)
}

fn check_process_waiters(ruby: &Ruby, core: &RefCell<SelectorCore>) -> Result<usize, Error> {
    let mut ready = Vec::new();
    {
        let mut core_ref = core.borrow_mut();
        let mut index = 0;
        while index < core_ref.process_waiters.len() {
            let status = {
                let waiter = &core_ref.process_waiters[index];
                process_status_wait(ruby, waiter.pid, waiter.flags)?
            };

            if status.is_nil() {
                index += 1;
                continue;
            }

            let waiter = core_ref.process_waiters.swap_remove(index);
            *waiter.result.lock().unwrap() = Some(status.into());
            ready.push(waiter.fiber);
        }
    }

    let mut count = 0;
    for fiber in ready {
        let fiber = ruby.get_inner(fiber);
        if alive_p(fiber)? {
            transfer_value(fiber, &[])?;
            count += 1;
        }
    }

    Ok(count)
}

#[cfg(unix)]
struct PollArguments {
    fds: Vec<libc::pollfd>,
    timeout: c_int,
    result: c_int,
    errno: c_int,
}

#[cfg(unix)]
unsafe extern "C" fn poll_without_gvl(data: *mut c_void) -> *mut c_void {
    let arguments = &mut *(data as *mut PollArguments);
    arguments.result = libc::poll(
        arguments.fds.as_mut_ptr(),
        arguments.fds.len() as libc::nfds_t,
        arguments.timeout,
    );
    if arguments.result == -1 {
        arguments.errno = errno();
    }
    std::ptr::null_mut()
}

#[cfg(unix)]
fn platform_poll(
    ruby: &Ruby,
    core: &RefCell<SelectorCore>,
    timeout: Option<i32>,
) -> Result<Vec<(RawDescriptor, i32)>, Error> {
    let mut fds = Vec::new();
    let mut fd_events = HashMap::<RawDescriptor, i16>::new();
    let interrupt_fd = core
        .borrow()
        .interrupt
        .as_ref()
        .map(|interrupt| interrupt.input);

    if let Some(fd) = interrupt_fd {
        fds.push(libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        });
    }

    {
        let core_ref = core.borrow();
        for waiter in &core_ref.io_waiters {
            let io = ruby.get_inner(waiter.io);
            if closed_p(io).unwrap_or(false) {
                continue;
            }

            let mut events = fd_events.remove(&waiter.fd).unwrap_or(0);
            if waiter.events & READABLE != 0 {
                events |= libc::POLLIN;
            }
            if waiter.events & PRIORITY != 0 {
                events |= libc::POLLPRI;
            }
            if waiter.events & WRITABLE != 0 {
                events |= libc::POLLOUT;
            }
            fd_events.insert(waiter.fd, events);
        }
    }

    for (fd, events) in fd_events {
        fds.push(libc::pollfd {
            fd,
            events,
            revents: 0,
        });
    }

    if fds.len() <= usize::from(interrupt_fd.is_some()) && timeout == Some(0) {
        return Ok(Vec::new());
    }

    let mut arguments = PollArguments {
        fds,
        timeout: timeout.unwrap_or(-1),
        result: 0,
        errno: 0,
    };

    {
        core.borrow_mut().blocked = true;
    }

    unsafe {
        rb_thread_call_without_gvl(
            poll_without_gvl,
            (&mut arguments as *mut PollArguments).cast::<c_void>(),
            (-1isize) as *mut c_void,
            std::ptr::null_mut(),
        );
    }

    {
        core.borrow_mut().blocked = false;
    }

    if arguments.result == -1 {
        if arguments.errno == libc::EINTR {
            return Ok(Vec::new());
        }

        return Err(errno_error("IO_Event_Selector_poll"));
    }

    let mut events = Vec::new();
    let interrupt_fd = interrupt_fd.unwrap_or(-1);
    for fd in &arguments.fds {
        if fd.revents == 0 {
            continue;
        }

        if fd.fd == interrupt_fd {
            if let Some(interrupt) = &core.borrow().interrupt {
                interrupt.clear()?;
            }
            continue;
        }

        let mut ready = 0;
        if fd.revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) != 0 {
            ready |= READABLE;
        }
        if fd.revents & libc::POLLPRI != 0 {
            ready |= PRIORITY;
        }
        if fd.revents & libc::POLLOUT != 0 {
            ready |= WRITABLE;
        }

        if ready != 0 {
            events.push((fd.fd, ready));
        }
    }

    Ok(events)
}

#[cfg(windows)]
fn platform_poll(
    _ruby: &Ruby,
    core: &RefCell<SelectorCore>,
    timeout: Option<i32>,
) -> Result<Vec<(RawDescriptor, i32)>, Error> {
    if let Some(timeout) = timeout {
        if timeout > 0 {
            std::thread::sleep(std::time::Duration::from_millis(timeout as u64));
        }
    }

    core.borrow_mut().blocked = false;
    Ok(Vec::new())
}

#[cfg(unix)]
enum BufferIoOperation {
    Read,
    Write,
    #[cfg(all(target_os = "linux", feature = "uring"))]
    PRead(i64),
    #[cfg(all(target_os = "linux", feature = "uring"))]
    PWrite(i64),
}

#[cfg(unix)]
impl BufferIoOperation {
    fn writes_to_buffer(&self) -> bool {
        match self {
            Self::Read => true,
            Self::Write => false,
            #[cfg(all(target_os = "linux", feature = "uring"))]
            Self::PRead(_) => true,
            #[cfg(all(target_os = "linux", feature = "uring"))]
            Self::PWrite(_) => false,
        }
    }

    fn events(&self) -> i32 {
        if self.writes_to_buffer() {
            READABLE
        } else {
            WRITABLE
        }
    }

    unsafe fn call(&mut self, fd: RawDescriptor, pointer: *mut c_void, length: usize) -> isize {
        match self {
            Self::Read => libc::read(fd, pointer, length),
            Self::Write => libc::write(fd, pointer.cast_const(), length),
            #[cfg(all(target_os = "linux", feature = "uring"))]
            Self::PRead(from) => libc::pread(fd, pointer, length, *from as libc::off_t),
            #[cfg(all(target_os = "linux", feature = "uring"))]
            Self::PWrite(from) => libc::pwrite(fd, pointer.cast_const(), length, *from as libc::off_t),
        }
    }

    #[cfg(all(target_os = "linux", feature = "uring"))]
    fn advance(&mut self, amount: usize) {
        match self {
            Self::PRead(from) | Self::PWrite(from) => *from += amount as i64,
            Self::Read | Self::Write => {}
        }
    }
}

#[cfg(unix)]
fn selector_io_buffer_syscall(
    ruby: &Ruby,
    core: &RefCell<SelectorCore>,
    fiber: Value,
    io: Value,
    buffer: Value,
    fd: RawDescriptor,
    mut length: usize,
    mut offset: usize,
    mut operation: BufferIoOperation,
) -> Result<Value, Error> {
    let (base, size) = if operation.writes_to_buffer() {
        let mut base = std::ptr::null_mut::<c_void>();
        let mut size = 0usize;
        unsafe {
            rb_io_buffer_get_bytes_for_writing(raw_from_value(buffer), &mut base, &mut size);
        }
        (base, size)
    } else {
        let mut base = std::ptr::null::<c_void>();
        let mut size = 0usize;
        unsafe {
            rb_io_buffer_get_bytes_for_reading(raw_from_value(buffer), &mut base, &mut size);
        }
        (base.cast_mut(), size)
    };

    if offset > size {
        return Ok((-libc::EINVAL).into_value_with(ruby));
    }

    let mut total = 0usize;
    let mut maximum_size = size - offset;
    loop {
        if maximum_size == 0 {
            break;
        }

        let result = unsafe {
            operation.call(
                fd,
                base.cast::<u8>().add(offset).cast::<c_void>(),
                maximum_size,
            )
        };

        if result < 0 {
            let errno = errno();
            if length > 0 && try_again(errno) {
                selector_io_wait(ruby, core, fiber, io, operation.events())?;
                continue;
            }

            return Ok((-(errno as i64)).into_value_with(ruby));
        }

        if result == 0 {
            break;
        }

        let result = result as usize;
        total += result;
        offset += result;

        #[cfg(all(target_os = "linux", feature = "uring"))]
        operation.advance(result);

        if result >= length {
            break;
        }

        length -= result;
        maximum_size = size - offset;
    }

    Ok(total.into_value_with(ruby))
}

fn selector_io_read(
    ruby: &Ruby,
    core: &RefCell<SelectorCore>,
    args: &[Value],
) -> Result<Value, Error> {
    if args.len() < 4 || args.len() > 5 {
        return Err(Error::new(
            ruby.exception_arg_error(),
            format!(
                "wrong number of arguments (given {}, expected 4..5)",
                args.len()
            ),
        ));
    }

    let fiber = args[0];
    let io = args[1];
    let buffer = args[2];
    let length = value_to_usize(args[3])?;
    let offset = if args.len() == 5 {
        value_to_usize(args[4])?
    } else {
        0
    };

    if offset > buffer_size(buffer)? {
        return Ok((-libc::EINVAL).into_value_with(ruby));
    }

    #[cfg(unix)]
    let fd = io_descriptor(io)?;
    #[cfg(not(unix))]
    let _fd = io_descriptor(io)?;
    #[cfg(unix)]
    let flags = set_nonblock(fd)?;

    let result = (|| -> Result<Value, Error> {
        #[cfg(unix)]
        {
            selector_io_buffer_syscall(
                ruby,
                core,
                fiber,
                io,
                buffer,
                fd,
                length,
                offset,
                BufferIoOperation::Read,
            )
        }

        #[cfg(not(unix))]
        {
            let mut length = length;
            let mut offset = offset;
            let mut total = 0usize;
            loop {
                let result: i64 = buffer.funcall("read", (io, 0usize, offset))?;
                if result < 0 {
                    let errno = -result as i32;
                    if length > 0 && try_again(errno) {
                        selector_io_wait(ruby, core, fiber, io, READABLE)?;
                        continue;
                    }

                    return Ok(result.into_value_with(ruby));
                }

                if result == 0 {
                    break;
                }

                let result = result as usize;
                total += result;
                offset += result;

                if result >= length {
                    break;
                }

                length -= result;
            }

            Ok(total.into_value_with(ruby))
        }
    })();

    #[cfg(unix)]
    restore_nonblock(fd, flags)?;

    result
}

fn selector_io_write(
    ruby: &Ruby,
    core: &RefCell<SelectorCore>,
    args: &[Value],
) -> Result<Value, Error> {
    if args.len() < 4 || args.len() > 5 {
        return Err(Error::new(
            ruby.exception_arg_error(),
            format!(
                "wrong number of arguments (given {}, expected 4..5)",
                args.len()
            ),
        ));
    }

    let fiber = args[0];
    let io = args[1];
    let buffer = args[2];
    let length = value_to_usize(args[3])?;
    let offset = if args.len() == 5 {
        value_to_usize(args[4])?
    } else {
        0
    };

    if offset > buffer_size(buffer)? {
        return Ok((-libc::EINVAL).into_value_with(ruby));
    }

    #[cfg(unix)]
    let fd = io_descriptor(io)?;
    #[cfg(not(unix))]
    let _fd = io_descriptor(io)?;
    #[cfg(unix)]
    let flags = set_nonblock(fd)?;

    let result = (|| -> Result<Value, Error> {
        #[cfg(unix)]
        {
            selector_io_buffer_syscall(
                ruby,
                core,
                fiber,
                io,
                buffer,
                fd,
                length,
                offset,
                BufferIoOperation::Write,
            )
        }

        #[cfg(not(unix))]
        {
            let mut length = length;
            let mut offset = offset;
            let mut total = 0usize;
            loop {
                let result: i64 = buffer.funcall("write", (io, 0usize, offset))?;
                if result < 0 {
                    let errno = -result as i32;
                    if length > 0 && try_again(errno) {
                        selector_io_wait(ruby, core, fiber, io, WRITABLE)?;
                        continue;
                    }

                    return Ok(result.into_value_with(ruby));
                }

                if result == 0 {
                    break;
                }

                let result = result as usize;
                total += result;
                offset += result;

                if result >= length {
                    break;
                }

                length -= result;
            }

            Ok(total.into_value_with(ruby))
        }
    })();

    #[cfg(unix)]
    restore_nonblock(fd, flags)?;

    result
}

#[cfg(all(target_os = "linux", feature = "uring"))]
fn selector_io_pread(
    ruby: &Ruby,
    core: &RefCell<SelectorCore>,
    args: &[Value],
) -> Result<Value, Error> {
    if args.len() != 6 {
        return Err(Error::new(
            ruby.exception_arg_error(),
            format!(
                "wrong number of arguments (given {}, expected 6)",
                args.len()
            ),
        ));
    }

    let fiber = args[0];
    let io = args[1];
    let buffer = args[2];
    let from = value_to_i64(args[3])?;
    let length = value_to_usize(args[4])?;
    let offset = value_to_usize(args[5])?;

    if offset > buffer_size(buffer)? {
        return Ok((-libc::EINVAL).into_value_with(ruby));
    }

    #[cfg(unix)]
    {
        let fd = io_descriptor(io)?;
        selector_io_buffer_syscall(
            ruby,
            core,
            fiber,
            io,
            buffer,
            fd,
            length,
            offset,
            BufferIoOperation::PRead(from),
        )
    }

    #[cfg(not(unix))]
    {
        let result: i64 = buffer.funcall("pread", (io, from, length, offset))?;
        Ok(result.into_value_with(ruby))
    }
}

#[cfg(all(target_os = "linux", feature = "uring"))]
fn selector_io_pwrite(
    ruby: &Ruby,
    core: &RefCell<SelectorCore>,
    args: &[Value],
) -> Result<Value, Error> {
    if args.len() != 6 {
        return Err(Error::new(
            ruby.exception_arg_error(),
            format!(
                "wrong number of arguments (given {}, expected 6)",
                args.len()
            ),
        ));
    }

    let fiber = args[0];
    let io = args[1];
    let buffer = args[2];
    let from = value_to_i64(args[3])?;
    let length = value_to_usize(args[4])?;
    let offset = value_to_usize(args[5])?;

    if offset > buffer_size(buffer)? {
        return Ok((-libc::EINVAL).into_value_with(ruby));
    }

    #[cfg(unix)]
    {
        let fd = io_descriptor(io)?;
        selector_io_buffer_syscall(
            ruby,
            core,
            fiber,
            io,
            buffer,
            fd,
            length,
            offset,
            BufferIoOperation::PWrite(from),
        )
    }

    #[cfg(not(unix))]
    {
        let result: i64 = buffer.funcall("pwrite", (io, from, length, offset))?;
        Ok(result.into_value_with(ruby))
    }
}

fn process_status_wait(ruby: &Ruby, pid: i64, flags: i32) -> Result<Value, Error> {
    let process = ruby.module_process();
    let wnohang: i32 = process.const_get("WNOHANG")?;
    let wait_flags = flags | wnohang;

    let status_class: Value = process.const_get("Status")?;
    let responds: bool = status_class.funcall("respond_to?", (Symbol::new("wait"),))?;
    if responds {
        return status_class.funcall("wait", (pid, wait_flags));
    }

    let result: Value = process.funcall("wait2", (pid, wait_flags))?;
    if result.is_nil() {
        return Ok(ruby.qnil().as_value());
    }

    let status: Value = result.funcall("last", ())?;
    Ok(status)
}

fn transfer_value(fiber: Value, args: &[Value]) -> Result<Value, Error> {
    if !alive_p(fiber)? {
        return Ok(Ruby::get_with(fiber).qnil().as_value());
    }

    fiber.funcall("transfer", args)
}

fn alive_p(value: Value) -> Result<bool, Error> {
    value.funcall("alive?", ())
}

fn closed_p(value: Value) -> Result<bool, Error> {
    value.funcall("closed?", ())
}

fn io_descriptor(io: Value) -> Result<RawDescriptor, Error> {
    let descriptor: i64 = io.funcall("fileno", ())?;
    Ok(descriptor as RawDescriptor)
}

fn buffer_size(buffer: Value) -> Result<usize, Error> {
    buffer.funcall("size", ())
}

fn value_to_usize(value: Value) -> Result<usize, Error> {
    usize::try_convert(value)
}

fn value_to_i64(value: Value) -> Result<i64, Error> {
    i64::try_convert(value)
}

fn duration_to_timeout_ms(duration: Value) -> Result<Option<i32>, Error> {
    if duration.is_nil() {
        return Ok(None);
    }

    let seconds: f64 = f64::try_convert(duration)?;
    if seconds <= 0.0 {
        return Ok(Some(0));
    }

    let milliseconds = (seconds * 1000.0).ceil();
    Ok(Some(milliseconds.min(i32::MAX as f64) as i32))
}

#[cfg(unix)]
fn set_nonblock(fd: RawDescriptor) -> Result<c_int, Error> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL, 0) };
    if flags == -1 {
        return Err(errno_error("IO_Event_Selector_nonblock_set:F_GETFL"));
    }

    if flags & libc::O_NONBLOCK == 0 {
        let result = unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
        if result == -1 {
            return Err(errno_error("IO_Event_Selector_nonblock_set:F_SETFL"));
        }
    }

    Ok(flags)
}

#[cfg(unix)]
fn restore_nonblock(fd: RawDescriptor, flags: c_int) -> Result<(), Error> {
    if flags & libc::O_NONBLOCK == 0 {
        let result = unsafe { libc::fcntl(fd, libc::F_SETFL, flags) };
        if result == -1 {
            return Err(errno_error("IO_Event_Selector_nonblock_restore:F_SETFL"));
        }
    }

    Ok(())
}

fn try_again(errno: i32) -> bool {
    errno == libc::EAGAIN || errno == libc::EWOULDBLOCK
}

#[cfg(all(unix, not(target_os = "linux")))]
fn errno() -> c_int {
    unsafe { *libc::__error() }
}

#[cfg(target_os = "linux")]
fn errno() -> c_int {
    unsafe { *libc::__errno_location() }
}

fn errno_error(message: &str) -> Error {
    Error::new(
        Ruby::get().unwrap().exception_system_call_error(),
        message.to_string(),
    )
}

fn same_value(a: Value, b: Value) -> bool {
    a.equal(b).unwrap_or(false)
}

fn raw_nil() -> rb_sys::VALUE {
    rb_sys::ruby_special_consts::RUBY_Qnil as rb_sys::VALUE
}

unsafe fn value_from_raw(value: rb_sys::VALUE) -> Value {
    std::mem::transmute::<rb_sys::VALUE, Value>(value)
}

fn raw_from_value(value: Value) -> rb_sys::VALUE {
    unsafe { std::mem::transmute::<Value, rb_sys::VALUE>(value) }
}

unsafe fn raw_funcall0(receiver: rb_sys::VALUE, method: &'static [u8]) -> Result<rb_sys::VALUE, Error> {
    magnus::rb_sys::protect(|| {
        let id = rb_sys::rb_intern(method.as_ptr().cast());
        rb_sys::rb_funcallv(receiver, id, 0, std::ptr::null())
    })
}

struct WorkerCall {
    operation: rb_sys::VALUE,
    scheduler: rb_sys::VALUE,
    blocker: rb_sys::VALUE,
    fiber: rb_sys::VALUE,
    completed: AtomicBool,
}

unsafe extern "C" fn worker_call_thread(data: *mut c_void) -> rb_sys::VALUE {
    let work = &*(data as *mut WorkerCall);

    let _ = raw_funcall0(work.operation, b"call\0");

    work.completed.store(true, Ordering::SeqCst);

    let _ = magnus::rb_sys::protect(|| {
        rb_sys::rb_fiber_scheduler_unblock(work.scheduler, work.blocker, work.fiber)
    });

    raw_nil()
}

#[cfg(unix)]
struct BusyOperationData {
    read_fd: c_int,
    write_fd: c_int,
    duration: f64,
    completed: AtomicBool,
    cancelled: AtomicBool,
    operation_result: AtomicI32,
}

#[cfg(unix)]
unsafe extern "C" fn busy_blocking_operation(data: *mut c_void) -> *mut c_void {
    let busy = &*(data as *mut BusyOperationData);

    let mut read_fds = std::mem::zeroed::<libc::fd_set>();
    libc::FD_ZERO(&mut read_fds);
    libc::FD_SET(busy.read_fd, &mut read_fds);

    let seconds = busy.duration.trunc().max(0.0);
    let microseconds = ((busy.duration - seconds) * 1_000_000.0).max(0.0);
    let mut timeout = libc::timeval {
        tv_sec: seconds as libc::time_t,
        tv_usec: microseconds as libc::suseconds_t,
    };

    let result = libc::select(
        busy.read_fd + 1,
        &mut read_fds,
        std::ptr::null_mut(),
        std::ptr::null_mut(),
        &mut timeout,
    );

    if result > 0 && libc::FD_ISSET(busy.read_fd, &read_fds) {
        let mut byte = 0u8;
        let _ = libc::read(
            busy.read_fd,
            (&mut byte as *mut u8).cast::<c_void>(),
            1,
        );
        busy.cancelled.store(true, Ordering::SeqCst);
        busy.operation_result.store(-1, Ordering::SeqCst);
        (-1isize) as *mut c_void
    } else if result == 0 {
        busy.operation_result.store(0, Ordering::SeqCst);
        std::ptr::null_mut()
    } else {
        busy.operation_result.store(-2, Ordering::SeqCst);
        (-2isize) as *mut c_void
    }
}

#[cfg(unix)]
unsafe extern "C" fn busy_unblock_function(data: *mut c_void) {
    let busy = &*(data as *mut BusyOperationData);
    busy.cancelled.store(true, Ordering::SeqCst);

    let byte = 1u8;
    let _ = libc::write(busy.write_fd, (&byte as *const u8).cast::<c_void>(), 1);
}

#[cfg(unix)]
unsafe fn busy_run_without_gvl(data: *mut BusyOperationData) {
    rb_sys::rb_nogvl(
        Some(busy_blocking_operation),
        data.cast::<c_void>(),
        Some(busy_unblock_function),
        data.cast::<c_void>(),
        (rb_sys::RB_NOGVL_UBF_ASYNC_SAFE | RB_NOGVL_OFFLOAD_SAFE) as c_int,
    );
    (*data).completed.store(true, Ordering::SeqCst);
}

#[cfg(unix)]
struct BusySchedulerWork {
    busy: *mut BusyOperationData,
    scheduler: rb_sys::VALUE,
    blocker: rb_sys::VALUE,
    fiber: rb_sys::VALUE,
}

#[cfg(unix)]
unsafe extern "C" fn busy_scheduler_thread(data: *mut c_void) -> rb_sys::VALUE {
    let work = &*(data as *mut BusySchedulerWork);
    busy_run_without_gvl(work.busy);

    let _ = magnus::rb_sys::protect(|| {
        rb_sys::rb_fiber_scheduler_unblock(work.scheduler, work.blocker, work.fiber)
    });

    raw_nil()
}

#[cfg(unix)]
fn worker_pool_busy_unix(ruby: &Ruby, duration: f64) -> Result<RHash, Error> {
    let mut pipe_fds = [0; 2];
    if unsafe { libc::pipe(pipe_fds.as_mut_ptr()) } != 0 {
        return Err(errno_error("IO_Event_WorkerPool_busy:pipe"));
    }

    let mut busy = BusyOperationData {
        read_fd: pipe_fds[0],
        write_fd: pipe_fds[1],
        duration,
        completed: AtomicBool::new(false),
        cancelled: AtomicBool::new(false),
        operation_result: AtomicI32::new(0),
    };

    let started = Instant::now();
    let mut exception = raw_nil();
    let mut interrupted = false;
    let scheduler = unsafe { rb_sys::rb_fiber_scheduler_current() };

    if scheduler == raw_nil() {
        unsafe {
            busy_run_without_gvl(&mut busy);
        }
    } else {
        let mut work = BusySchedulerWork {
            busy: &mut busy,
            scheduler,
            blocker: raw_nil(),
            fiber: unsafe { rb_sys::rb_fiber_current() },
        };

        let work_ptr = (&mut work as *mut BusySchedulerWork).cast::<c_void>();
        let thread = unsafe {
            magnus::rb_sys::protect(|| {
                rb_sys::rb_thread_create(Some(busy_scheduler_thread), work_ptr)
            })?
        };

        while !busy.completed.load(Ordering::SeqCst) {
            let result = unsafe {
                magnus::rb_sys::protect(|| {
                    rb_sys::rb_fiber_scheduler_block(scheduler, raw_nil(), raw_nil())
                })
            };

            if result.is_err() {
                interrupted = true;
                unsafe {
                    if exception == raw_nil() {
                        exception = rb_sys::rb_errinfo();
                    }
                    busy_unblock_function((&mut busy as *mut BusyOperationData).cast::<c_void>());
                    rb_sys::rb_set_errinfo(raw_nil());
                }
            }
        }

        let _ = unsafe { raw_funcall0(thread, b"join\0") };
    }

    let elapsed = started.elapsed().as_secs_f64();
    let result = busy.operation_result.load(Ordering::SeqCst);
    let cancelled = busy.cancelled.load(Ordering::SeqCst);

    unsafe {
        libc::close(pipe_fds[0]);
        libc::close(pipe_fds[1]);
    }

    let hash = ruby.hash_new();
    hash.aset(Symbol::new("duration"), duration)?;
    hash.aset(Symbol::new("elapsed"), elapsed)?;

    if interrupted {
        let exception_value = if exception == raw_nil() {
            ruby.exception_standard_error()
                .new_instance(("interrupted",))?
                .as_value()
        } else {
            unsafe { value_from_raw(exception) }
        };

        hash.aset(Symbol::new("result"), Symbol::new("exception"))?;
        hash.aset(Symbol::new("cancelled"), true)?;
        hash.aset(Symbol::new("exception"), exception_value)?;
    } else if result == -1 || cancelled {
        hash.aset(Symbol::new("result"), Symbol::new("cancelled"))?;
        hash.aset(Symbol::new("cancelled"), true)?;
    } else if result == 0 {
        hash.aset(Symbol::new("result"), Symbol::new("completed"))?;
        hash.aset(Symbol::new("cancelled"), false)?;
    } else {
        hash.aset(Symbol::new("result"), Symbol::new("error"))?;
        hash.aset(Symbol::new("cancelled"), false)?;
    }

    Ok(hash)
}

#[derive(Default)]
struct WorkerPoolState {
    maximum_worker_count: usize,
    call_count: usize,
    completed_count: usize,
    cancelled_count: usize,
    shutdown: bool,
}

#[derive(TypedData, Default)]
#[magnus(class = "IO::Event::WorkerPool", free_immediately, size)]
struct WorkerPool {
    state: RefCell<WorkerPoolState>,
}

impl DataTypeFunctions for WorkerPool {}

impl WorkerPool {
    fn initialize(&self, args: &[Value]) -> Result<Value, Error> {
        let ruby = Ruby::get().unwrap();
        let mut maximum_worker_count = 1usize;

        if let Some(kwargs) = args.last().and_then(|value| RHash::from_value(*value)) {
            if let Some(value) = kwargs.get(Symbol::new("maximum_worker_count")) {
                if !value.is_nil() {
                    maximum_worker_count = usize::try_convert(value)?;
                }
            }
        }

        if maximum_worker_count == 0 {
            return Err(Error::new(
                ruby.exception_arg_error(),
                "maximum_worker_count must be greater than 0!",
            ));
        }

        *self.state.borrow_mut() = WorkerPoolState {
            maximum_worker_count,
            call_count: 0,
            completed_count: 0,
            cancelled_count: 0,
            shutdown: false,
        };

        Ok(ruby.qnil().as_value())
    }

    fn call(&self, operation: Value) -> Result<bool, Error> {
        let ruby = Ruby::get_with(operation);
        let mut state = self.state.borrow_mut();
        if state.shutdown {
            return Err(Error::new(
                ruby.exception_runtime_error(),
                "Worker pool is shut down!",
            ));
        }

        state.call_count += 1;
        drop(state);

        let callable: bool = operation.funcall("respond_to?", (Symbol::new("call"),))?;
        if !callable {
            return Err(Error::new(
                ruby.exception_arg_error(),
                "Invalid blocking operation!",
            ));
        }

        let scheduler = unsafe { rb_sys::rb_fiber_scheduler_current() };
        if scheduler == raw_nil() {
            return Err(Error::new(
                ruby.exception_runtime_error(),
                "WorkerPool requires a fiber scheduler!",
            ));
        }

        let operation_value = raw_from_value(operation);
        let mut work = Box::new(WorkerCall {
            operation: operation_value,
            scheduler,
            blocker: operation_value,
            fiber: unsafe { rb_sys::rb_fiber_current() },
            completed: AtomicBool::new(false),
        });

        let work_ptr = (&mut *work as *mut WorkerCall).cast::<c_void>();
        let thread = unsafe {
            magnus::rb_sys::protect(|| rb_sys::rb_thread_create(Some(worker_call_thread), work_ptr))?
        };

        let mut error = None;
        while !work.completed.load(Ordering::SeqCst) {
            let result = unsafe {
                magnus::rb_sys::protect(|| {
                    rb_sys::rb_fiber_scheduler_block(scheduler, operation_value, raw_nil())
                })
            };

            if let Err(error_value) = result {
                if error.is_none() {
                    error = Some(error_value);
                }

                let _ = unsafe { raw_funcall0(thread, b"kill\0") };
            }
        }

        let _ = unsafe { raw_funcall0(thread, b"join\0") };

        if let Some(error) = error {
            self.state.borrow_mut().cancelled_count += 1;
            return Err(error);
        }

        self.state.borrow_mut().completed_count += 1;
        Ok(true)
    }

    fn close(&self) -> Value {
        let ruby = Ruby::get().unwrap();
        let mut state = self.state.borrow_mut();
        state.shutdown = true;
        ruby.qnil().as_value()
    }

    fn statistics(&self) -> RHash {
        let ruby = Ruby::get().unwrap();
        let state = self.state.borrow();
        let hash = ruby.hash_new();
        hash.aset(
            Symbol::new("current_worker_count"),
            if state.shutdown {
                0usize
            } else {
                state.maximum_worker_count
            },
        )
        .unwrap();
        hash.aset(Symbol::new("maximum_worker_count"), state.maximum_worker_count)
            .unwrap();
        hash.aset(Symbol::new("call_count"), state.call_count).unwrap();
        hash.aset(Symbol::new("completed_count"), state.completed_count)
            .unwrap();
        hash.aset(Symbol::new("cancelled_count"), state.cancelled_count)
            .unwrap();
        hash.aset(Symbol::new("shutdown"), state.shutdown).unwrap();
        hash.aset(Symbol::new("current_queue_size"), 0usize).unwrap();
        hash
    }

    fn busy(args: &[Value]) -> Result<RHash, Error> {
        let ruby = Ruby::get().unwrap();
        let mut duration = 0.1f64;

        if let Some(kwargs) = args.last().and_then(|value| RHash::from_value(*value)) {
            if let Some(value) = kwargs.get(Symbol::new("duration")) {
                duration = value.funcall("to_f", ())?;
            }
        }

        #[cfg(unix)]
        {
            return worker_pool_busy_unix(&ruby, duration);
        }

        #[cfg(not(unix))]
        {
            std::thread::sleep(std::time::Duration::from_secs_f64(duration));

            let hash = ruby.hash_new();
            hash.aset(Symbol::new("cancelled"), false)?;
            hash.aset(Symbol::new("result"), Symbol::new("completed"))?;
            hash.aset(Symbol::new("duration"), duration)?;
            Ok(hash)
        }
    }
}

macro_rules! define_common_selector_methods {
    ($class:expr, $type:ident) => {{
        $class.define_method("initialize", method!($type::initialize, 1))?;
        $class.define_method("loop", method!($type::loop_fiber, 0))?;
        $class.define_method("idle_duration", method!($type::idle_duration, 0))?;
        $class.define_method("transfer", method!($type::transfer, 0))?;
        $class.define_method("resume", method!($type::resume, -1))?;
        $class.define_method("yield", method!($type::yield_fiber, 0))?;
        $class.define_method("push", method!($type::push, 1))?;
        $class.define_method("raise", method!($type::raise, -1))?;
        $class.define_method("ready?", method!($type::ready_p, 0))?;
        $class.define_method("select", method!($type::select, 1))?;
        $class.define_method("wakeup", method!($type::wakeup, 0))?;
        $class.define_method("close", method!($type::close, 0))?;
        $class.define_method("io_wait", method!($type::io_wait, 3))?;
        $class.define_method("io_read", method!($type::io_read, -1))?;
        $class.define_method("io_write", method!($type::io_write, -1))?;
        $class.define_method("process_wait", method!($type::process_wait, 3))?;
    }};
}

#[magnus::init(name = "IO_Event")]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let io = ruby.class_io();
    let io_event = io.define_module("Event")?;
    let selector = io_event.define_module("Selector")?;

    ruby.eval::<Value>("require 'io/nonblock'")?;
    ruby.eval::<Value>(
        r#"
            module IO::Event::Selector
              def self.nonblock(io, &block)
                io.nonblock(&block)
              rescue Errno::EBADF
                yield
              end
            end
        "#,
    )?;

    #[cfg(target_os = "linux")]
    {
        let epoll = selector.define_class("EPoll", ruby.class_object())?;
        epoll.define_alloc_func::<EPollSelector>();
        define_common_selector_methods!(epoll, EPollSelector);
    }

    #[cfg(any(
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly"
    ))]
    {
        let kqueue = selector.define_class("KQueue", ruby.class_object())?;
        kqueue.define_alloc_func::<KQueueSelector>();
        define_common_selector_methods!(kqueue, KQueueSelector);
    }

    #[cfg(windows)]
    {
        let iocp = selector.define_class("IOCP", ruby.class_object())?;
        iocp.define_alloc_func::<IOCPSelector>();
        define_common_selector_methods!(iocp, IOCPSelector);
    }

    #[cfg(all(target_os = "linux", feature = "uring"))]
    {
        let uring = selector.define_class("URing", ruby.class_object())?;
        uring.define_alloc_func::<URingSelector>();
        define_common_selector_methods!(uring, URingSelector);
        uring.define_method("io_pread", method!(URingSelector::io_pread, -1))?;
        uring.define_method("io_pwrite", method!(URingSelector::io_pwrite, -1))?;
        uring.define_method("io_close", method!(URingSelector::io_close, 1))?;
    }

    let worker_pool = io_event.define_class("WorkerPool", ruby.class_object())?;
    worker_pool.define_alloc_func::<WorkerPool>();
    worker_pool.define_method("initialize", method!(WorkerPool::initialize, -1))?;
    worker_pool.define_method("call", method!(WorkerPool::call, 1))?;
    worker_pool.define_method("close", method!(WorkerPool::close, 0))?;
    worker_pool.define_method("statistics", method!(WorkerPool::statistics, 0))?;
    worker_pool.define_singleton_method("busy", function!(WorkerPool::busy, -1))?;

    let _runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build();

    Ok(())
}
