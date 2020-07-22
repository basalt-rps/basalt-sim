use rand::{thread_rng, Rng};
use std::collections::HashSet;
use structopt::StructOpt;

use crate::net::{App, PeerRef, Network};
use crate::net::Metrics as NetMetrics;
use crate::rps;
use crate::util::sample_nocopy;

pub enum Msg {
    SelfNotif,
    Step1(Vec<PeerRef>),
    Step2(Vec<PeerRef>),
}

#[derive(Default, Clone, StructOpt, Debug)]
pub struct Init {
    /// Number of Byzantine nodes
    #[structopt(short = "t", long = "num-byzantines")]
    pub n_byzantine: usize,

    /// Peer sampling view size
    #[structopt(short = "v", long = "view-size")]
    pub view_size: usize,

    /// Number of samples returned
    #[structopt(short = "k", long = "n-samples")]
    pub count: usize,

    /// Sampling period
    #[structopt(short = "r", long = "sample-interval")]
    pub period: usize,
}

pub struct RPS {
    params: Init,

    my_id: PeerRef,
    counter: usize,
    is_byzantine: bool,
    view: Vec<PeerRef>,
}

pub struct Metrics {
    n_procs: usize,
    n_byzantine_neighbors: usize,
    n_isolated: usize,
}

impl NetMetrics for Metrics {
    fn empty() -> Self {
        Metrics {
            n_procs: 0,
            n_byzantine_neighbors: 0,
            n_isolated: 0,
        }
    }
    fn net_combine(&mut self, other: &Self) {
        self.n_procs += other.n_procs;
        self.n_byzantine_neighbors += other.n_byzantine_neighbors;
        self.n_isolated += other.n_isolated;
    }
    fn headers() -> Vec<&'static str> {
        vec!["avgByzN", "n_isolated"]
    }
    fn values(&self) -> Vec<String> {
        vec![
            format!("{:.2}", 
               (self.n_byzantine_neighbors as f32) / (self.n_procs as f32)),
            format!("{}", self.n_isolated)
        ]
    }
}

type Net<'a> = &'a mut dyn Network<Msg>;

impl App for RPS {
    type Init = Init;
    type Msg = Msg;
    type Metrics = Metrics;

    fn new() -> Self {
        Self {
            params: Init::default(),
            my_id: 0,
            counter: 0,
            is_byzantine: false,
            view: Vec::new(),
        }
    }
    
    fn init(&mut self, id: PeerRef, net: Net, init: &Self::Init) {
        self.params = init.clone();

        self.my_id = id;
        self.is_byzantine = id < init.n_byzantine;
        if self.is_byzantine {
            self.view = (0..self.params.view_size).collect();
        } else {
            self.view = net.sample_peers(self.params.view_size);
        }
        net.send(id, Msg::SelfNotif);
    }

    fn handle(&mut self, net: Net, from: PeerRef, msg: &Self::Msg) {
        let mut rng = thread_rng();
        let integrate = match msg {
            Msg::SelfNotif => {
                let i = rng.gen_range(0, self.view.len());
                net.send(self.view[i], Msg::Step1(self.view.clone()));
                net.send(self.my_id, Msg::SelfNotif);
                None
            },
            Msg::Step1(in_view) => {
                net.send(from, Msg::Step2(self.view.clone()));
                Some(in_view)
            }
            Msg::Step2(in_view) => {
                Some(in_view)
            }
        };
        if !self.is_byzantine {
            if let Some(in_view) = integrate {
                let mut tmp = self.view.iter().cloned().collect::<HashSet<_>>();
                for x in in_view.iter() {
                    tmp.insert(*x);
                }
                self.view = tmp.iter().cloned().collect::<Vec<_>>();
                rng.shuffle(&mut self.view[..]);
                while self.view.len() > self.params.view_size {
                    self.view.pop();
                }
            }
        }
    }

    fn metrics(&mut self, _net: Net) -> Self::Metrics {
        if self.is_byzantine {
            Self::Metrics::empty()
        } else {
            let nbn = self.view.iter().filter(|x| **x < self.params.n_byzantine).count();
            Self::Metrics{
                n_procs: 1,
                n_byzantine_neighbors: nbn,
                n_isolated: if nbn == self.view.len() { 1 } else { 0 },
            }
        }
    }
}

impl rps::RPS for RPS {
    fn get_samples(&mut self) -> Vec<PeerRef> {
        self.counter = self.counter + 1;
        if (self.counter + self.my_id) % self.params.period == 0 {
            sample_nocopy(&mut self.view[..], self.params.count)
        } else {
            vec![]
        }
    }
    fn clear_samples(&mut self) {
    }
}

