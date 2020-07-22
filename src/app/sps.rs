use rand::{thread_rng, Rng};
use std::collections::{HashMap};
use structopt::StructOpt;

use crate::net::{App, PeerRef, Network};
use crate::net::Metrics as NetMetrics;
use crate::util::{either_or_if_both, sample_nocopy};
use crate::rps::RPS;
use crate::graph::ByzConnGraph;


pub enum Msg {
    SelfNotif,
    Request(Vec<(PeerRef, i64)>),
    Reply(Vec<(PeerRef, i64)>),
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
    #[structopt(short = "r", long = "sampling-frequency")]
    pub sampling_frequency: Option<u64>,

    /// Replacement count: replace k (this parameter) neighbours every r time units
    #[structopt(short = "k", long = "sampling-count", default_value = "1")]
    pub sampling_count: usize,

    /// Peer sampling view size
    #[structopt(short = "v", long = "view-size")]
    pub view_size: usize,

    /// Number of exchanges every round
    #[structopt(short = "x", long = "num-exchanges", default_value = "4")]
    pub num_exchanges: usize,

    /// Time delta between two exchanges
    #[structopt(short = "d", long = "exchange-interval", default_value = "4")]
    pub exchange_interval: usize,

    /// TTL0 parameter
    #[structopt(long = "ttl0", default_value = "4")]
    pub ttl0: i64,

    /// WLIST parameter
    #[structopt(long = "wlist-max", default_value = "100")]
    pub wlist_max: usize,

    /// Enable detailed graph statistics
    #[structopt(short = "G", long = "graph-stats")]
    pub graph_stats: bool,
}

pub struct SPS {
    params: Init,

    my_id: PeerRef,
    is_byzantine: bool,

    view: HashMap<PeerRef, i64>,         // i64: ts
    ptable: HashMap<PeerRef, PEntry>,
    wlist: HashMap<PeerRef, i64>,       // i64: ts
    done: bool,
    check: PeerRef,
    request_set: Vec<PeerRef>,
    hits_mu: f64,
    hits_sigma: f64,

    out_samples: Vec<PeerRef>,

    n_received: usize,
    n_byzantine_received: usize,
}

struct PEntry {
    ts: i64,
    ttl: i64,
    hits: usize,
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

impl SPS {
    fn compute_blacklist(&self) -> Vec<PeerRef> {
        let mut ret = vec![];
        for (peer, pentry) in self.ptable.iter() {
            if pentry.hits as f64 > self.hits_mu + self.hits_sigma {
                ret.push(*peer);
            }
        }
        ret
    }

    fn blacklisted(&self, peer: PeerRef) -> bool {
        if let Some(pentry) = self.ptable.get(&peer) {
            if pentry.hits as f64 > self.hits_mu + self.hits_sigma {
                return true;
            }
        }
        false
    }

    fn merge_view(&mut self, view: &[(PeerRef, i64)]) {
        for (peer, ts) in view.iter() {
            self.view.insert(*peer, *ts);
        }
        self.view = keep_most_recent(std::mem::replace(&mut self.view, HashMap::new()),
                                          self.params.view_size);
    }

    fn update_statistics(&mut self, view: &[(PeerRef, i64)]) {
        for (q, ts) in view.iter() {
            if let Some(pentry) = self.ptable.get_mut(q) {
                pentry.hits += 1;
                pentry.ttl += 1;
            } else {
                self.ptable.insert(*q, PEntry{
                    ts: *ts,
                    hits: 1,
                    ttl: self.params.ttl0,
                });
            }
        }
        // Calculate statisaouzcs
        self.hits_mu = 0.;
        for (_, pentry) in self.ptable.iter() {
            self.hits_mu += pentry.hits as f64;
        }
        self.hits_mu /= self.ptable.len() as f64;
        self.hits_sigma = 0.;
        for (_, pentry) in self.ptable.iter() {
            let x = pentry.hits as f64 - self.hits_mu;
            self.hits_sigma += x*x;
        }
        self.hits_sigma = (self.hits_sigma / self.ptable.len() as f64).sqrt();

        let blacklist = self.compute_blacklist();
        self.wlist.retain(|k, _| !blacklist.contains(k));

        let mut new_view = self.view.clone();
        for (q, _ts) in self.view.iter() {
            if blacklist.contains(q) {
                // Find a whitelisted peer not yet in the view
                for (q2, ts2) in self.wlist.iter() {
                    if !new_view.contains_key(q2) {
                        new_view.remove(q);
                        new_view.insert(*q2, *ts2);
                        break;
                    }
                }
            }
        }
        self.view = new_view;
    }


    fn limit_wlist(&mut self) {
        self.wlist = keep_most_recent(std::mem::replace(&mut self.wlist, HashMap::new()),
                                           self.params.wlist_max);
    }
}

fn keep_most_recent(mut x: HashMap<PeerRef, i64>, count: usize) -> HashMap<PeerRef, i64> {
    let mut all_view = x.drain().collect::<Vec<(PeerRef, i64)>>();
    all_view.sort_by_key(|(_, ts)| -ts);
    while all_view.len() > count{
        all_view.pop();
    }
    return all_view.drain(..).collect::<HashMap<PeerRef, i64>>();
}

impl App for SPS {
    type Init = Init;
    type Msg = Msg;
    type Metrics = Metrics;

    fn new() -> Self {
        Self {
            params: Init::default(),

            my_id: 0,
            is_byzantine: false,

            view: HashMap::new(),
            ptable: HashMap::new(),
            wlist: HashMap::new(),
            done: false,
            check: 0,
            request_set: Vec::new(),
            hits_mu: 1000.0,
            hits_sigma: 1000.0,

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
            for p in net.sample_peers(self.params.view_size) {
                self.view.insert(p, 0);
            }
        }
        net.send(id, Msg::SelfNotif);
    }

    fn handle(&mut self, net: Net, from: PeerRef, msg: &Self::Msg) {
        if self.is_byzantine {
            let mut byzantines = (0..self.params.n_byzantine).collect::<Vec<_>>();
            match msg {
                Msg::SelfNotif => {
                    if net.time() >= self.params.attack_start_time {
                        for p in net.sample_peers(self.params.byzantine_flood_factor) {
                            let sent_view = sample_nocopy(&mut byzantines[..], self.params.view_size)
                                .iter()
                                .map(|x| (*x, net.time() as i64))
                                .collect::<Vec<_>>();
                            net.send(p, Msg::Request(sent_view));
                        }
                    }
                    net.send(self.my_id, Msg::SelfNotif);
                },
                Msg::Request(_) => {
                    if net.time() >= self.params.attack_start_time {
                        let sent_view = sample_nocopy(&mut byzantines[..], self.params.view_size)
                            .iter()
                            .map(|x| (*x, net.time() as i64))
                            .collect::<Vec<_>>();
                        net.send(from, Msg::Reply(sent_view));
                    } else {
                        let sent_view = net.sample_peers(self.params.view_size)
                            .iter()
                            .map(|x| (*x, net.time() as i64))
                            .collect::<Vec<_>>();
                        net.send(from, Msg::Reply(sent_view));
                    }
                },
                _ => (),
            }
        } else {
            match msg {
                Msg::SelfNotif => {
                    let mut view = self.view.iter()
                        .map(|(k, _v)| *k)
                        .collect::<Vec<_>>();

                    if (self.my_id + net.time() as usize) % self.params.exchange_interval == 0 {
                        self.done = false;

                        let mut blacklist = self.compute_blacklist();

                        // Send a request to some peers
                        self.request_set = sample_nocopy(&mut view[..], self.params.num_exchanges);

                        let mut sent = self.view.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>();
                        sent.push((self.my_id, net.time() as i64));

                        for p in self.request_set.iter() {
                            if !blacklist.contains(p) {
                                net.send(*p, Msg::Request(sent.clone()));
                            }
                        }

                        // Send a check to a blacklisted peer
                        if blacklist.len() > 0 {
                            self.check = sample_nocopy(&mut blacklist[..], 1)[0];
                            net.send(self.check, Msg::Request(sent));
                        }

                        // Decrease ptable TTL values
                        let mut new_ptable = HashMap::new();
                        for (peer, pentry) in self.ptable.iter() {
                            if pentry.ttl <= 1 {
                                self.wlist.insert(*peer, pentry.ts);
                            } else {
                                new_ptable.insert(*peer, PEntry{
                                    ts: pentry.ts,
                                    ttl: pentry.ttl - 1,
                                    hits: pentry.hits,
                                });
                            }
                        }
                        self.limit_wlist();
                        self.ptable = new_ptable;
                    }

                    if let Some(rf) = self.params.sampling_frequency {
                        if (self.my_id as u64 + net.time()) % rf == 0 {
                            if self.out_samples.len() < 200 {
                                self.out_samples.extend(sample_nocopy(&mut view[..], self.params.sampling_count));
                            }
                        }
                    }

                    net.send(self.my_id, Msg::SelfNotif);
                },
                Msg::Request(peer_list) => {
                    // stats
                    self.n_received += peer_list.len();
                    self.n_byzantine_received += peer_list.iter()
                        .filter(|(x, _)| *x < self.params.n_byzantine)
                        .count();

                    let mut sent = self.view.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>();
                    sent.push((self.my_id, net.time() as i64));
                    net.send(from, Msg::Reply(sent));
                    if !self.blacklisted(from) {
                        self.merge_view(&peer_list[..]);
                    }
                }
                Msg::Reply(peer_list) => {
                    // stats
                    self.n_received += peer_list.len();
                    self.n_byzantine_received += peer_list.iter()
                        .filter(|(x, _)| *x < self.params.n_byzantine)
                        .count();

                    let mut rng = thread_rng();
                    let toss = rng.gen_range::<f64>(0., 1.);
                    let thresh = 1. / (self.params.num_exchanges as f64);
                    if self.request_set.contains(&from) && !self.blacklisted(from) && !self.done && toss < thresh {
                        self.done = true;
                        self.merge_view(&peer_list[..]);
                    } else {
                        self.update_statistics(&peer_list[..]);
                    }
                    if from == self.check {
                        let blacklist = self.compute_blacklist();
                        if peer_list.iter()
                            .all(|(p, _)| !blacklist.contains(p)) {
                            if let Some(tup) = self.ptable.remove(&from) {
                                self.wlist.insert(from, tup.ts);
                                self.limit_wlist();
                            }
                        }
                    }
                }
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
                .filter(|(entry, _)| **entry < self.params.n_byzantine).count();

            let graph = if self.params.graph_stats {
                let neighs = self.view.iter().map(|(x, _)| *x).collect::<Vec<_>>();
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

impl RPS for SPS {
    fn get_samples(&mut self) -> Vec<PeerRef> {
        std::mem::replace(&mut self.out_samples, Vec::new())
    }
    fn clear_samples(&mut self) {
        self.out_samples.clear();
    }
}
