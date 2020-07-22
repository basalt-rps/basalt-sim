use rand::{thread_rng, Rng};
use structopt::StructOpt;

use crate::net::{App, PeerRef, Network};
use crate::net::Metrics as NetMetrics;
use crate::util::{either_or_if_both, hash, sample_nocopy};
use crate::rps::RPS;
use crate::graph::ByzConnGraph;


pub enum Msg {
    SelfNotif,
    Pull,
    Push(Vec<PeerRef>),
}

#[derive(Clone, Default, StructOpt, Debug)]
pub struct Init {
    /// Number of Byzantine nodes
    #[structopt(short = "t", long = "num-byzantines")]
    pub n_byzantine: usize,

    /// Byzantine flood factor
    #[structopt(short = "f", long = "byzantine-flood-factor")]
    pub byzantine_flood_factor: usize,

    /// Byzantine attack start time
    #[structopt(short = "s", long = "attack-start-time", default_value = "0")]
    pub attack_start_time: u64,

    /// Replacement frequency: replace k neighbor every r (this paramter) time units
    #[structopt(short = "r", long = "replacement-frequency")]
    pub replacement_frequency: Option<u64>,

    /// Replacement count: replace k (this parameter) neighbours every r time units
    #[structopt(short = "k", long = "replacement-count", default_value = "1")]
    pub replacement_count: usize,

    /// Peer sampling view size
    #[structopt(short = "v", long = "view-size")]
    pub view_size: usize,

    /// Number of initally known nodes of the network (uniformly sampled)
    #[structopt(short = "i", long = "num-initial-samples")]
    pub initial_uniform_samples: usize,

    /// Use minimum hit peer selection strategy or pure randomness
    #[structopt(short = "H", long = "use-hit-counter")]
    pub use_hit_counter: bool,

    /// Enable detailed graph statistics
    #[structopt(short = "G", long = "graph-stats")]
    pub graph_stats: bool,
}

pub struct Basalt {
    params: Init,

    my_id: PeerRef,
    is_byzantine: bool,

    view: Vec<ViewEntry>,

    out_samples: Vec<PeerRef>,

    n_received: usize,
    n_byzantine_received: usize,
}

struct ViewEntry {
    seed: u64,
    peer: PeerRef,
    hits: i64,
}

pub struct Metrics {
    n_procs: usize,

    n_byzantine_received: usize,
    n_received: usize,

    n_byzantine_neighbors: usize,
    min_byzantine_neighbors: Option<i64>,
    max_byzantine_neighbors: Option<i64>,
    n_isolated: usize,

    graph: ByzConnGraph,
}

impl NetMetrics for Metrics {
    fn empty() -> Self {
        Metrics {
            n_procs: 0,
            n_byzantine_received: 0,
            n_received: 0,
            n_byzantine_neighbors: 0,
            min_byzantine_neighbors: None,
            max_byzantine_neighbors: None,
            n_isolated: 0,
            graph: ByzConnGraph::new(),
        }
    }
    fn net_combine(&mut self, other: &Self) {
        self.n_procs += other.n_procs;

        self.n_byzantine_received += other.n_byzantine_received;
        self.n_received += other.n_received;

        self.n_byzantine_neighbors += other.n_byzantine_neighbors;
        self.max_byzantine_neighbors = either_or_if_both(
            &self.max_byzantine_neighbors,
            &other.max_byzantine_neighbors,
            |a, b| std::cmp::max(*a, *b));
        self.min_byzantine_neighbors = either_or_if_both(
            &self.min_byzantine_neighbors,
            &other.min_byzantine_neighbors,
            |a, b| std::cmp::min(*a, *b));
        self.n_isolated += other.n_isolated;

        self.graph.combine(&other.graph);
    }
    fn headers() -> Vec<&'static str> {
        vec![
            "avgRecv",
            "avgByzRecv",
            "pByzRecv",
            "avgByzN",
            "min",
            "max",
            "n_isolated",
            "cluscoeff",
            "MPL",
            "id_min", "id_d1", "id_q1", "id_med", "id_q3", "id_d9", "id_max",
        ]
    }
    fn values(&self) -> Vec<String> {
        // Clustering coefficient
        let cluscoeff = self.graph.clustering_coeff();

        // In-degree quartiles (for correct nodes)
        let ind = self.graph.indegree_dist(self.n_procs);

        // Average path length estimation
        let mpl = self.graph.mean_path_length(self.n_procs);

        vec![
            format!("{:.2}",
                   (self.n_received as f32) / (self.n_procs as f32)),
            format!("{:.2}",
                   (self.n_byzantine_received as f32) / (self.n_procs as f32)),
            format!("{:.4}",
                   (self.n_byzantine_received as f32) / (self.n_received as f32)),
            format!("{:.2}",
                   (self.n_byzantine_neighbors as f32) / (self.n_procs as f32)),
            format!("{}", self.min_byzantine_neighbors.unwrap_or(-1)),
            format!("{}", self.max_byzantine_neighbors.unwrap_or(-1)),
            format!("{}", self.n_isolated),

            format!("{:.4}", cluscoeff),
            format!("{:.4}", mpl),
            format!("{}", ind[0]),
            format!("{}", ind[ind.len()/10]),
            format!("{}", ind[ind.len()/4]),
            format!("{}", ind[ind.len()/2]),
            format!("{}", ind[3*ind.len()/4]),
            format!("{}", ind[9*ind.len()/10]),
            format!("{}", ind[ind.len()-1]),
        ]
    }
}

type Net<'a> = &'a mut dyn Network<Msg>;



impl Basalt {
    fn update_samples(&mut self, candidates: &[PeerRef]) {
        for i in 0..self.view.len() {
            self.update_sample(i, candidates);
        }
    }

    fn update_sample(&mut self, i: usize, candidates: &[PeerRef]) {
        let entry = &mut self.view[i];
        let mut prev_hash = hash(entry.seed, entry.peer);

        for candidate in candidates.iter() {
            if *candidate == entry.peer {
                entry.hits += 1;
            } else {
                let new_hash = hash(entry.seed, *candidate);
                if new_hash < prev_hash {
                    entry.peer = *candidate;
                    entry.hits = 1;
                    prev_hash = new_hash;
                }
            }
        }
    }

    fn get_exchange_peer(&mut self, rng: &mut rand::ThreadRng) -> PeerRef {
        if self.params.use_hit_counter {
            let mut ret = 0;
            for i in 1..self.view.len() {
                if self.view[i].hits < self.view[ret].hits {
                    ret = i;
                }
            }
            self.view[ret].hits += 1;
            self.view[ret].peer
        } else {
            self.view[rng.gen_range(0, self.view.len())].peer
        }
    }
}

impl App for Basalt {
    type Init = Init;
    type Msg = Msg;
    type Metrics = Metrics;

    fn new() -> Self {
        Self {
            params: Init::default(),

            my_id: 0,
            is_byzantine: false,
            view: Vec::new(),
            out_samples: Vec::new(),

            n_received: 0,
            n_byzantine_received: 0,
        }
    }
    
    fn init(&mut self, id: PeerRef, net: Net, init: &Self::Init) {
        self.my_id = id;
        self.params = init.clone();

        self.is_byzantine = id < init.n_byzantine;
        if !self.is_byzantine {
            let mut rng = thread_rng();
            self.view = (0..self.params.view_size)
                .map(|_| ViewEntry{
                    seed: rng.gen_range(0, std::u64::MAX),
                    peer: id,
                    hits: 1
                }).collect();

            let initial_samples = net.sample_peers(self.params.initial_uniform_samples);
            self.update_samples(&initial_samples[..]);
        }
        net.send(id, Msg::SelfNotif);
    }

    fn handle(&mut self, net: Net, from: PeerRef, msg: &Self::Msg) {
        if self.is_byzantine {
            let mut byzantines = (0..self.params.n_byzantine).collect::<Vec<_>>();
            match msg {
                Msg::SelfNotif => {
                    net.send(self.my_id, Msg::SelfNotif);
                    if net.time() >= self.params.attack_start_time {
                        net.sample_peers(self.params.byzantine_flood_factor)
                            .iter()
                            .for_each(|p| net.send(*p, Msg::Push(sample_nocopy(&mut byzantines[..], self.params.view_size))));
                    }
                },
                Msg::Pull => {
                    net.send(from, Msg::Push(sample_nocopy(&mut byzantines[..], self.params.view_size)));
                },
                _ => (),
            }
        } else {
            let mut rng = thread_rng();
            let view = self.view.iter()
                .map(|entry| entry.peer)
                .collect::<Vec<_>>();
            match msg {
                Msg::SelfNotif => {
                    if let Some(rf) = self.params.replacement_frequency {
                        if (self.my_id as u64 + net.time()) % rf == 0 {
                            for k in 0..self.params.replacement_count {
                                let i_replace = ((net.time() / rf) as usize * self.params.replacement_count + k) % self.view.len();
                                if self.out_samples.len() < 200 {
                                    self.out_samples.push(self.view[i_replace].peer);
                                }
                                self.view[i_replace].seed = rng.gen_range(0, std::u64::MAX);
                                self.view[i_replace].hits = 1;
                                self.update_sample(i_replace, &view[..]);
                            }
                        }
                    }

                    let pull_from = self.get_exchange_peer(&mut rng);
                    net.send(pull_from, Msg::Pull);

                    let push_to = self.get_exchange_peer(&mut rng);
                    net.send(push_to, Msg::Push(view.clone()));

                    net.send(self.my_id, Msg::SelfNotif);
                },
                Msg::Pull => {
                    net.send(from, Msg::Push(view.clone()));
                },
                Msg::Push(lst) => {
                    self.n_received += lst.len();
                    self.n_byzantine_received += lst.iter()
                        .filter(|x| **x < self.params.n_byzantine)
                        .count();
                    self.update_samples(&lst[..]);
                    self.update_samples(&[from]);
                },
            }
        }
    }

    fn metrics(&mut self, _net: Net) -> Self::Metrics {
        if self.is_byzantine {
            let mut metrics = Self::Metrics::empty();

            if self.params.graph_stats {
                let neighs = (0..self.params.n_byzantine).collect::<Vec<_>>();
                metrics.graph = ByzConnGraph::peer_new(self.params.n_byzantine,
                                                       self.my_id,
                                                       neighs);
            }

            metrics
        } else {
            let nbn = self.view.iter()
                .filter(|entry| entry.peer < self.params.n_byzantine).count();

            let graph = if self.params.graph_stats {
                let neighs = self.view.iter().map(|x| x.peer).collect::<Vec<_>>();
                ByzConnGraph::peer_new(self.params.n_byzantine, self.my_id, neighs)
            } else {
                ByzConnGraph::new()
            };

            let ret = Self::Metrics{
                n_procs: 1,
                n_received: self.n_received,
                n_byzantine_received: self.n_byzantine_received,
                n_byzantine_neighbors: nbn,
                n_isolated: if nbn == self.view.len() { 1 } else { 0 },
                min_byzantine_neighbors: Some(nbn as i64),
                max_byzantine_neighbors: Some(nbn as i64),
                graph,
            };
            self.n_received = 0;
            self.n_byzantine_received = 0;
            ret
        }
    }
}

impl RPS for Basalt {
    fn get_samples(&mut self) -> Vec<PeerRef> {
        std::mem::replace(&mut self.out_samples, Vec::new())
    }
    fn clear_samples(&mut self) {
        self.out_samples.clear();
    }
}
