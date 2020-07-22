use rayon::prelude::*;

use rand::{thread_rng, Rng};

//use super::metrics::Metric;

const DEBUG: bool = false;
const STEP_LENGTH: u64 = 1;

pub type PeerRef = usize;

pub trait Metrics {
    fn empty() -> Self;
    fn net_combine(&mut self, other: &Self);
    fn headers() -> Vec<&'static str>;
    fn values(&self) -> Vec<String>;
}

pub trait Network<Msg> {
    fn sample_peers(&self, n: usize) -> Vec<PeerRef>;
    fn send(&mut self, to: PeerRef, msg: Msg);
    fn time(&self) -> u64;
}

pub trait App {
    type Init: Sync + Send;
    type Msg: Send;
    type Metrics: Metrics + Send;

    fn new() -> Self
        where Self: Sized;

    fn init(&mut self, my_id: PeerRef, network: &mut dyn Network<Self::Msg>, init: &Self::Init)
        where Self: Sized;

    fn handle(&mut self, network: &mut dyn Network<Self::Msg>, from: PeerRef, msg: &Self::Msg)
        where Self: Sized;

    fn metrics(&mut self, network: &mut dyn Network<Self::Msg>) -> Self::Metrics
        where Self: Sized;
}

struct Message<Msg> {
    from: PeerRef,
    to: PeerRef,
    arrival_time: u64,
    msg: Msg,
}

struct NetHandler<A> where A: App + Send {
    id: PeerRef,
    nproc: usize,
    time: u64,
    outbox: Vec<Box<Message<A::Msg>>>,
    metrics: A::Metrics,
    n_recv: usize,
}

impl<A> Network<A::Msg> for NetHandler<A> where A: App + Send {
    fn sample_peers(&self, n: usize) -> Vec<PeerRef> {
        let mut rng = thread_rng();
        if n <= self.nproc / 10 {
            let mut res = Vec::new();
            while res.len() < n {
                let i = rng.gen_range(0, self.nproc);
                if i != self.id && !res.contains(&i) {
                    res.push(i);
                }
            }
            res
        } else {
            let mut vec = (0..self.nproc).collect::<Vec<_>>();
            rng.shuffle(&mut vec[..]);
            vec.iter().cloned().take(n).collect::<Vec<_>>()
        }
    }

    fn send(&mut self, to: PeerRef, msg: A::Msg) {
        //let latency = (100 + (self.id + to) % 100) as u64;
        let latency = STEP_LENGTH;
        self.outbox.push(Box::new(Message{
            from: self.id,
            to: to,
            arrival_time: self.time + latency,
            msg: msg
        }));
    }

    fn time(&self) -> u64 {
        self.time
    }
}

pub struct Proc<A> where A: App + Send {
    id: PeerRef,
    inbox: Vec<Box<Message<A::Msg>>>,
    pub state: A,
}

pub struct Simulator<A> where A: App + Send {
    nproc: usize,

    step_length: u64,
    time: u64,
    pub processes: Vec<Proc<A>>,

    metrics: A::Metrics,
    n_sent: usize,
    n_recv: usize,
}

impl<A: App + Send> Simulator<A> {
    pub fn new(nproc: usize, init: &A::Init) -> Self {
        let mut net = Self {
            nproc,
            step_length: STEP_LENGTH,
            time: 0,
            processes: Vec::new(),
            metrics: A::Metrics::empty(),
            n_sent: 0,
            n_recv: 0,
        };
        for i in 0..nproc {
            net.processes.push(Proc{
                id: i, 
                inbox: Vec::new(),
                state: A::new()
            });
        }
        let out = net.processes.par_iter_mut()
            .map(|proc| {
                let mut handler = NetHandler{
                    id: proc.id,
                    nproc,
                    time: 0,
                    outbox: Vec::new(),
                    metrics: A::Metrics::empty(),
                    n_recv: 0,
                };
                proc.state.init(proc.id, &mut handler, init);
                handler.metrics = proc.state.metrics(&mut handler);
                handler
            })
            .collect::<Vec<_>>();
        net.incorporate(out);
        net
    }

    fn incorporate(&mut self, mut out: Vec<NetHandler<A>>) {
        if DEBUG {
            eprintln!("Begin metric collection...");
        }

        self.metrics = out.par_iter_mut()
            .map(|x| std::mem::replace(&mut x.metrics, A::Metrics::empty()))
            .reduce(|| A::Metrics::empty(),
                    |mut a, b| { a.net_combine(&b); a });

        self.n_recv = out.par_iter_mut()
            .map(|x| x.n_recv)
            .reduce(|| 0, |a, b| a + b);

        if DEBUG {
            eprintln!("Begin message exchange (1)...");
        }
        const N_CHUNKS: usize = 128;
        let chunk_size: usize = ((self.nproc - 1) / N_CHUNKS) + 1;
        let mut msgs = out.par_chunks_mut(chunk_size*4)
            .map(|chunk| {
                let mut msgs_by_dest_chunk = (0..N_CHUNKS).map(|_| Vec::new())
                    .collect::<Vec<_>>();
                for proc in chunk.iter_mut() {
                    for msg in proc.outbox.drain(..) {
                        let dest_chunk = msg.to / chunk_size;
                        msgs_by_dest_chunk[dest_chunk].push(msg);
                    }
                }
                msgs_by_dest_chunk
            }).collect::<Vec<_>>();

        if DEBUG {
            eprintln!("Begin message exchange (2)...");
        }
        self.n_sent = 0;
        let mut msgs_by_dest_chunk = (0..N_CHUNKS).map(|_| Vec::new())
            .collect::<Vec<_>>();
        for mut bit in msgs.drain(..) {
            for (chunk_messages, chunk) in bit.drain(..).zip(0..N_CHUNKS) {
                self.n_sent += chunk_messages.len();
                msgs_by_dest_chunk[chunk].push(chunk_messages);
            }
        }
    
        if DEBUG {
            eprintln!("Begin message exchange (3)...");
        }
        self.processes.par_chunks_mut(chunk_size)
            .zip(msgs_by_dest_chunk.par_iter_mut())
            .for_each(|(procs, msgs)| {
                let first_proc = procs[0].id;
                for mut msgx in msgs.drain(..) {
                    for msg in msgx.drain(..) {
                        let i = msg.to - first_proc;
                        assert!(procs[i].id == msg.to);
                        procs[i].inbox.push(msg);
                    }
                }
            });

        // Slow (non-parallel) version of message exchange
        if false {
            for i in 0..self.nproc {
                for msg_box in out[i].outbox.drain(..) {
                    assert!(msg_box.from == i);
                    self.processes[msg_box.to].inbox.push(msg_box);
                }
            }
        }
    }

    pub fn print_header(&self) {
        print!("{:10} {:10} {:10}", "time", "n_sent", "n_recv");
        for v in A::Metrics::headers() {
            print!(" {:10}", v);
        }
        println!("");
    }

    pub fn print_metrics(&self) {
        print!("{:<10} {:<10} {:<10}", self.time, self.n_sent, self.n_recv);
        for v in self.metrics.values() {
            print!(" {:10}", v);
        }
        println!("");
    }

    pub fn step(&mut self) {
        if DEBUG {
            eprintln!("Begin step...");
        }

        let nproc = self.nproc;
        let until_time = self.time + self.step_length;
        let out = self.processes.par_iter_mut()
            .map(|proc| {
                let (mut to_handle, remaining): (Vec<_>, Vec<_>) = proc.inbox.drain(..).partition(|msg| msg.arrival_time <= until_time);
                proc.inbox = remaining;

                let mut handler = NetHandler{
                    id: proc.id,
                    nproc,
                    time: 0,
                    outbox: Vec::new(),
                    metrics: A::Metrics::empty(),
                    n_recv: to_handle.len(),
                };
                to_handle.sort_by(|a, b| a.arrival_time.cmp(&b.arrival_time));
                for message in to_handle {
                    handler.time = message.arrival_time;
                    proc.state.handle(&mut handler, message.from, &message.msg);
                }
                handler.metrics = proc.state.metrics(&mut handler);
                handler
            })
            .collect::<Vec<_>>();
        self.incorporate(out);
        self.time = until_time;
    }
}

