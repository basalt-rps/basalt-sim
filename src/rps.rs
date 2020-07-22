use super::net::{App, PeerRef, Network};
use super::net::Metrics as NetMetrics;

use structopt::StructOpt;

use super::util::sample_nocopy;

pub struct EmptyMetrics;
impl NetMetrics for EmptyMetrics {
    fn empty() -> Self {
        Self
    }
    fn net_combine(&mut self, _other: &Self) {
    }
    fn headers() -> Vec<&'static str> {
        vec![]
    }
    fn values(&self) -> Vec<String> {
        vec![]
    }
}

pub trait RPS {
    fn get_samples(&mut self) -> Vec<PeerRef>;
    fn clear_samples(&mut self);
}

#[derive(Clone, Default, StructOpt, Debug)]
pub struct OracleInit {
    #[structopt(skip)]
    pub n_nodes: usize,

    /// Number of samples returned
    #[structopt(short = "k", long = "n-samples")]
    pub count: usize,

    /// Sampling period
    #[structopt(short = "r", long = "sample-interval")]
    pub period: usize,
}


pub struct Oracle {
    my_id: PeerRef,
    params: OracleInit,
    counter: usize,
    nodes: Vec<PeerRef>,
}

impl App for Oracle {
    type Init = OracleInit;
    type Msg = ();
    type Metrics = EmptyMetrics;

    fn new() -> Self {
        Self{
            my_id: 0,
            params: OracleInit::default(),
            counter: 0,
            nodes: Vec::new(),
        }
    }

    fn init(&mut self, my_id: PeerRef, _network: &mut dyn Network<Self::Msg>, init: &Self::Init) {
        self.my_id = my_id;
        self.params = init.clone();
        self.nodes = (0..self.params.n_nodes).collect();
    }

    fn handle(&mut self, _network: &mut dyn Network<Self::Msg>, _from: PeerRef, _msg: &Self::Msg) {
    }

    fn metrics(&mut self, _network: &mut dyn Network<Self::Msg>) -> Self::Metrics {
        Self::Metrics::empty()
    }
}

impl RPS for Oracle {
    fn get_samples(&mut self) -> Vec<PeerRef> {
        self.counter = self.counter + 1;
        if (self.counter + self.my_id) % self.params.period == 0 {
            sample_nocopy(&mut self.nodes[..], self.params.count)
        } else {
            vec![]
        }
    }
    fn clear_samples(&mut self) {
    }
}

