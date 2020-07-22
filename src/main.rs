mod net;
mod util;
mod graph;
mod rps;

mod app;

use std::sync::{Arc, RwLock};

use structopt::StructOpt;
use net::{Simulator, App};

#[derive(StructOpt, Debug)]
#[structopt(name = "bignetrs")]
pub struct Opt {
    /// Iteration number for a repeated experiment (ignored)
    #[structopt(short = "i", long = "iteration", default_value = "0")]
    iteration: usize,

    /// Number of simulation steps
    #[structopt(short = "T", long = "time", default_value = "100")]
    n_steps: usize,
    /// Number of nodes
    #[structopt(short = "n", long = "nodes", default_value = "1000")]
    nodes: usize,

    /// Show random peer samples instead of metrics after a certain time
    #[structopt(short="R", long = "random-samples")]
    random_samples: Option<usize>,

    #[structopt(subcommand)]
    app: WhichApp,
}

#[derive(StructOpt, Debug)]
pub enum WhichApp {
    /// Simple Random Peer Sampling
    #[structopt(name = "rps")]
    RPS(app::rps::Init),

    /// Brahms RPS
    #[structopt(name = "brahms")]
    Brahms(app::brahms::Init),

    /// Secure Peer Sampling
    #[structopt(name = "sps")]
    SPS(app::sps::Init),

    /// Basalt RPS without hit counter mechanism
    #[structopt(name = "basalt-simple")]
    BasaltSimple(app::basalt::Init),

    /// Basalt RPS
    #[structopt(name = "basalt")]
    Basalt(app::basalt::Init),

    /// Avalanche consensus algorithm using any RPS
    #[structopt(name = "avalanche")]
    Avalanche(app::avalanche::InitCmd),
}

fn main() {
    let opt = Opt::from_args();
    match opt.app {
        WhichApp::RPS(pp) => {
            if let Some(rs) = opt.random_samples {
                sim_rps_rng::<app::rps::RPS>(opt.n_steps, opt.nodes, &pp, rs);
            } else {
                sim::<app::rps::RPS>(opt.n_steps, opt.nodes, &pp);
            }
        }
        WhichApp::Brahms(pp) => {
            if let Some(rs) = opt.random_samples {
                sim_rps_rng::<app::brahms::Brahms>(opt.n_steps, opt.nodes, &pp, rs);
            } else {
                sim::<app::brahms::Brahms>(opt.n_steps, opt.nodes, &pp);
            }   
        }
        WhichApp::SPS(pp) => {
            if let Some(rs) = opt.random_samples {
                sim_rps_rng::<app::sps::SPS>(opt.n_steps, opt.nodes, &pp, rs);
            } else {
                sim::<app::sps::SPS>(opt.n_steps, opt.nodes, &pp);
            }   
        }
        WhichApp::BasaltSimple(mut pp) => {
            pp.use_hit_counter = false;
            if let Some(rs) = opt.random_samples {
                sim_rps_rng::<app::basalt::Basalt>(opt.n_steps, opt.nodes, &pp, rs);
            } else {
                sim::<app::basalt::Basalt>(opt.n_steps, opt.nodes, &pp);
            }
        }
        WhichApp::Basalt(mut pp) => {
            pp.use_hit_counter = true;
            if let Some(rs) = opt.random_samples {
                sim_rps_rng::<app::basalt::Basalt>(opt.n_steps, opt.nodes, &pp, rs);
            } else {
                sim::<app::basalt::Basalt>(opt.n_steps, opt.nodes, &pp);
            }
        }
        WhichApp::Avalanche(pp) => {
            let shared_counter = Arc::new(RwLock::new((0, 0)));
            match pp.rps {
                app::avalanche::WhichRPS::Oracle(mut prps) => {
                    prps.n_nodes = opt.nodes;
                    let init = app::avalanche::Init::<rps::Oracle>{
                        args: pp.args,
                        rps_args: prps,
                        shared_counter,
                    };
                    sim::<app::avalanche::Avalanche<rps::Oracle>>(opt.n_steps, opt.nodes, &init);
                }
                app::avalanche::WhichRPS::SPS(prps) => {
                    let init = app::avalanche::Init::<app::sps::SPS>{
                        args: pp.args,
                        rps_args: prps,
                        shared_counter,
                    };
                    sim::<app::avalanche::Avalanche<app::sps::SPS>>(opt.n_steps, opt.nodes, &init);
                }
                app::avalanche::WhichRPS::Brahms(prps) => {
                    let init = app::avalanche::Init::<app::brahms::Brahms>{
                        args: pp.args,
                        rps_args: prps,
                        shared_counter,
                    };
                    sim::<app::avalanche::Avalanche<app::brahms::Brahms>>(opt.n_steps, opt.nodes, &init);
                }
                app::avalanche::WhichRPS::BasaltSimple(mut prps) => {
                    prps.use_hit_counter = false;
                    let init = app::avalanche::Init::<app::basalt::Basalt>{
                        args: pp.args,
                        rps_args: prps,
                        shared_counter,
                    };
                    sim::<app::avalanche::Avalanche<app::basalt::Basalt>>(opt.n_steps, opt.nodes, &init);
                }
                app::avalanche::WhichRPS::Basalt(mut prps) => {
                    prps.use_hit_counter = true;
                    let init = app::avalanche::Init::<app::basalt::Basalt>{
                        args: pp.args,
                        rps_args: prps,
                        shared_counter,
                    };
                    sim::<app::avalanche::Avalanche<app::basalt::Basalt>>(opt.n_steps, opt.nodes, &init);
                }
            }
            
        }
    }
}

fn sim<A: App + Send>(nsteps: usize, nproc: usize, init: &A::Init) {
    let mut net = Simulator::<A>::new(nproc, init);

    net.print_header();
    net.print_metrics();

    for _step in 0..nsteps {
        net.step();
        net.print_metrics();
    }
}

fn sim_rps_rng<A: App + rps::RPS + Send>(nsteps: usize, nproc: usize, init: &A::Init, first_output_round: usize) {
    let mut net = Simulator::<A>::new(nproc, init);

    for step in 0..nsteps {
        net.step();
        if step >= first_output_round {
            let i = nproc - 1;
            //for i in (nproc/2)..nproc {
                for r in net.processes[i].state.get_samples() {
                    println!("{}", r);
                }
            //}
        }
    }
}
