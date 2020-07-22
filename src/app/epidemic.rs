use crate::net::{App, PeerRef, Network, self};

pub type Msg = bool;

pub struct Epidemic {
    contaminated: bool,
}

pub struct Metrics {
    n_contaminated: usize,
}

impl net::Metrics for Metrics {
    fn empty() -> Self {
        Metrics{
            n_contaminated: 0
        }
    }
    fn net_combine(&mut self, other: &Self) {
        self.n_contaminated += other.n_contaminated;
    }
    fn headers() -> Vec<&'static str> {
        vec!["n_contaminated"]
    }
    fn values(&self) -> Vec<String> {
        vec![
            format!("{}", self.n_contaminated),
        ]
    }
}

type Net<'a> = &'a mut dyn Network<Msg>;

impl App for Epidemic {
    type Init = ();
    type Msg = Msg;
    type Metrics = Metrics;

    fn new() -> Self {
        Self{ contaminated: false }
    }

    fn init(&mut self, id: PeerRef, net: Net, _init: &Self::Init) {
        if id == 0 {
            net.sample_peers(10).iter().for_each(|x| net.send(*x, true));
            self.contaminated = true;
        }
    }

    fn handle(&mut self, net: Net, _from: PeerRef, msg: &Self::Msg) {
        if *msg && !self.contaminated {
            net.sample_peers(10).iter().for_each(|x| net.send(*x, true));
            self.contaminated = true;
        }
    }

    fn metrics(&mut self, _net: Net) -> Self::Metrics {
        if self.contaminated {
            Self::Metrics{n_contaminated: 1}
        } else {
            Self::Metrics{n_contaminated: 0}
        }
    }
}
