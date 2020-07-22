use structopt::StructOpt;
use std::collections::{HashSet, HashMap};
use std::sync::{Arc, RwLock};

use crate::net::{App, PeerRef, Network};
use crate::net::Metrics as NetMetrics;
use super::{brahms, sps, basalt};
use crate::rps::{RPS, OracleInit};
use crate::util::{either_or_if_both};

pub enum Msg<T: App> {
    SelfNotif,
    Pull,
    Push(bool),
    RPSMsg(T::Msg)
}

#[derive(Clone, Default, StructOpt, Debug)]
pub struct InitArgs {
    /// Number of Byzantine nodes
    #[structopt(short = "t", long = "num-byzantines")]
    pub n_byzantine: usize,

    /// Number of disagreeing correct nodes
    #[structopt(short = "d", long ="num-disagree", default_value = "0")]
    pub n_disagreeing: usize,

    /// Scenario
    #[structopt(short = "S", long = "scenario")]
    pub scenario: Scenario,

    /// Sample size
    #[structopt(short = "k", long = "sample-size")]
    pub k: usize,

    /// Threshold alpha times k
    #[structopt(short = "a", long = "alpha-k")]
    pub alpha_k: usize,

    /// Threshold beta times k
    #[structopt(short = "b", long = "beta")]
    pub beta: f32,

    /// Threshold theta times k
    #[structopt(short = "c", long = "theta")]
    pub theta: usize,

    /// Avalanche algorithm start time
    #[structopt(short = "s", long = "start-time", default_value = "0")]
    pub start_time: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Scenario {
    Absent,
    Disagreeing,
    Adaptive,
}

impl Default for Scenario {
    fn default() -> Self {
        Self::Absent
    }
}

impl std::str::FromStr for Scenario {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "absent" => Ok(Self::Absent),
            "disagreeing" => Ok(Self::Disagreeing),
            "adaptive" => Ok(Self::Adaptive),
            _ => Err("invalid byzantine behaviour"),
        }
    }
}

#[derive(Clone, StructOpt, Debug)]
pub struct InitCmd {
    #[structopt(flatten)]
    pub args: InitArgs,

    #[structopt(subcommand)]
    pub rps: WhichRPS,
}

pub struct Init<T: App + RPS> {
    pub args: InitArgs,
    pub rps_args: T::Init,
    pub shared_counter: Arc<RwLock<(usize, usize)>>,
}


#[derive(Clone, StructOpt, Debug)]
pub enum WhichRPS {
    /// Oracle RPS
    #[structopt(name = "oracle")]
    Oracle(OracleInit),

    /// Brahms RPS
    #[structopt(name = "brahms")]
    Brahms(brahms::Init),

    /// Secure Peer Sampling
    #[structopt(name = "sps")]
    SPS(sps::Init),

    /// Basalt RPS without hit counter
    #[structopt(name = "basalt-simple")]
    BasaltSimple(basalt::Init),

    /// Basalt RPS
    #[structopt(name = "basalt")]
    Basalt(basalt::Init),
}

pub struct Avalanche<T: App + RPS> {
    params: InitArgs,
    rps: T,
    shared_counter: Option<Arc<RwLock<(usize, usize)>>>,
    
    my_id: PeerRef,
    is_byzantine: bool,

    rps_set: Vec<PeerRef>,
    query_set: HashSet<PeerRef>,
    reply_set: HashMap<PeerRef, bool>,
    value: bool,
    timeout: usize,
    counter: usize,
    decided: Option<bool>,
}

pub struct Metrics<T: App> {
    n_procs: usize,

    n_true: usize,
    n_false: usize,

    n_decided_true: usize,
    n_decided_false: usize,

    rps_metrics: T::Metrics,

    shared_counter: Option<Arc<RwLock<(usize, usize)>>>,
}

type Net<'a, T> = &'a mut dyn Network<Msg<T>>;

impl<T: App> NetMetrics for Metrics<T> {
    fn empty() -> Self {
        Metrics {
            n_procs: 0,
            n_true: 0,
            n_false: 0,
            n_decided_true: 0,
            n_decided_false: 0,
            rps_metrics: T::Metrics::empty(),
            shared_counter: None,
        }
    }
    fn net_combine(&mut self, other: &Self) {
        self.n_procs += other.n_procs;
        self.n_true += other.n_true;
        self.n_false += other.n_false;
        self.n_decided_true += other.n_decided_true;
        self.n_decided_false += other.n_decided_false;
        self.rps_metrics.net_combine(&other.rps_metrics);
        self.shared_counter = either_or_if_both(&self.shared_counter, &other.shared_counter, |x, _y| x.clone());
    }
    fn headers() -> Vec<&'static str> {
        let mut ret = vec![
            "nTrue",
            "nFalse",
            "decTrue",
            "decFalse",
        ];
        ret.extend(T::Metrics::headers());
        ret
    }
    fn values(&self) -> Vec<String> {
        let mut sc = self.shared_counter.as_ref().unwrap().write().unwrap();
        sc.0 = self.n_false;
        sc.1 = self.n_true;
        let mut ret = vec![
            format!("{}", self.n_true),
            format!("{}", self.n_false),
            format!("{}", self.n_decided_true),
            format!("{}", self.n_decided_false),
        ];
        ret.extend(self.rps_metrics.values());
        ret
    }
}

struct NetProxy<'a, T: App> {
    net: &'a mut dyn Network<Msg<T>>,
}

impl<'a, T: App> Network<T::Msg> for NetProxy<'a, T> {
    fn sample_peers(&self, n: usize) -> Vec<PeerRef> {
        self.net.sample_peers(n)
    }
    fn send(&mut self, to: PeerRef, msg: T::Msg) {
        self.net.send(to, Msg::RPSMsg(msg))
    }
    fn time(&self) -> u64 {
        self.net.time()
    }
}

impl<T> App for Avalanche<T>
    where T: App + RPS, <T as App>::Init: Default
{
    type Init = Init<T>;
    type Msg = Msg<T>;
    type Metrics = Metrics<T>;

    fn new() -> Self {
        Self {
            params: InitArgs::default(),
            shared_counter: None,

            rps: T::new(),

            my_id: 0,
            is_byzantine: false,

            rps_set: Vec::new(),
            query_set: HashSet::new(),
            reply_set: HashMap::new(),
            value: false,
            counter: 0,
            timeout: 0,
            decided: None,
        }
    }

    fn init(&mut self, id: PeerRef, net: Net<T>, init: &Self::Init) {
        self.rps.init(id, &mut NetProxy{net}, &init.rps_args);

        self.my_id = id;
        self.params = init.args.clone();
        self.shared_counter = Some(init.shared_counter.clone());

        self.is_byzantine = id < self.params.n_byzantine;
        if !self.is_byzantine {
            net.send(id, Msg::SelfNotif);
            if self.my_id - self.params.n_byzantine < self.params.n_disagreeing {
                self.value = true;
            } else {
                self.value = false;
            }
        }

    }

    fn handle(&mut self, net: Net<T>, from: PeerRef, msg: &Self::Msg) {
        if let Msg::RPSMsg(mm) = msg {
            self.rps.handle(&mut NetProxy{net}, from, mm);
            return;
        }
        if self.is_byzantine {
            match msg {
                Msg::Pull => {
                    match self.params.scenario {
                        Scenario::Absent => (),
                        Scenario::Disagreeing => {
                            net.send(from, Msg::Push(true));
                        }
                        Scenario::Adaptive => {
                            let sc = self.shared_counter.as_ref().unwrap().read().unwrap();
                            if sc.0 > sc.1 {
                                net.send(from, Msg::Push(true));
                            } else {
                                net.send(from, Msg::Push(false));
                            }
                        }
                    }
                }
                Msg::RPSMsg(_) => unreachable!(),
                _ => (),
            }
        } else {
            match msg {
                Msg::SelfNotif => {
                    if self.decided.is_none() {
                        if net.time() < self.params.start_time {
                            self.rps.clear_samples();
                        } else {
                            self.rps_set.extend(self.rps.get_samples());
                        }

                        if self.timeout == 0
                            && net.time() >= self.params.start_time
                            && self.rps_set.len() >= self.params.k
                        {
                            self.query_set.clear();
                            self.reply_set.clear();
                            while self.query_set.len() < self.params.k && !self.rps_set.is_empty() {
                                let p = self.rps_set.pop().unwrap();
                                self.query_set.insert(p);
                                net.send(p, Msg::Pull);
                            }
                            self.timeout = 2;
                        } else if self.timeout > 0 {
                            self.timeout -= 1;
                        }
                    }
                    net.send(self.my_id, Msg::SelfNotif);
                },
                Msg::Pull => {
                    if let Some(d) = self.decided {
                        net.send(from, Msg::Push(d))
                    } else {
                        net.send(from, Msg::Push(self.value))
                    }
                }
                Msg::Push(v) => {
                    if self.query_set.contains(&from) {
                        self.reply_set.insert(from, *v);
                    }
                    if self.reply_set.len() >= self.params.alpha_k {
                        let count_true = self.reply_set.iter()
                            .filter(|(_, v)| **v == true)
                            .count();
                        let count_false = self.reply_set.iter()
                            .filter(|(_, v)| **v == false)
                            .count();
                        let mut proposal = None;
                        let thresh = self.params.beta * self.reply_set.len() as f32;
                        if count_true as f32 > thresh {
                            proposal = Some(true)
                        }
                        if count_false as f32 > thresh {
                            proposal = Some(false)
                        }

                        if let Some(prop) = proposal {
                            if self.value == prop {
                                self.counter += 1;
                                if self.counter >= self.params.theta {
                                    self.decided = Some(self.value)
                                }
                            } else {
                                if self.counter > 0 {
                                    self.counter = self.counter - 1;
                                }
                                if self.counter == 0 {
                                    self.value = prop;
                                }
                            }
                        }
                    }
                }
                Msg::RPSMsg(_) => unreachable!(),
            }
        }
    }

    fn metrics(&mut self, net: Net<T>) -> Self::Metrics {
        if self.is_byzantine {
            let mut ret = Self::Metrics::empty();
            ret.rps_metrics = self.rps.metrics(&mut NetProxy{net});
            ret
        } else {
            let mut metrics = Self::Metrics::empty();
            metrics.rps_metrics = self.rps.metrics(&mut NetProxy{net});
            metrics.n_procs = 1;
            metrics.shared_counter = self.shared_counter.clone();
            if self.value {
                metrics.n_true = 1;
            } else {
                metrics.n_false = 1;
            }
            if let Some(d) = self.decided {
                if d {
                    metrics.n_decided_true = 1;
                } else {
                    metrics.n_decided_false = 1;
                }
            }
            metrics
        }
    }
}
