use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use rand::{thread_rng, Rng};
use rayon::prelude::*;

use super::net::PeerRef;
use super::util::either_or_if_both;

pub struct ByzConnGraph {
    n_byzantine: Option<usize>,
    graph: HashMap<PeerRef, Arc<Vec<PeerRef>>>,
}

impl ByzConnGraph {
    pub fn new() -> Self {
        Self{
            n_byzantine: None,
            graph: HashMap::new(),
        }
    }
    pub fn peer_new(n_byzantine: usize, peer: PeerRef, mut neighbors: Vec<PeerRef>) -> Self {
        let mut ret = Self {
            n_byzantine: Some(n_byzantine),
            graph: HashMap::new(),
        };
        neighbors.sort();
        neighbors.dedup();
        ret.graph.insert(peer, Arc::new(neighbors));
        ret
    }

    pub fn combine(&mut self, other: &Self) {
        self.n_byzantine = either_or_if_both(
            &self.n_byzantine,
            &other.n_byzantine,
            |a, b| { assert!(*a == *b); *a });

        for (k, v) in other.graph.iter() {
            self.graph.insert(*k, v.clone());
        }
    }

    pub fn clustering_coeff(&self) -> f64 {
        if self.graph.is_empty() {
            return 0.;
        }

        let n_byzantine = self.n_byzantine.unwrap();

        let local_coeffs = self.graph.par_iter()
            .filter(|(x, _)| **x >= n_byzantine)
            .map(|(_, neighbors)| {
            let mut links = 0;
            for n in neighbors.iter() {
                if let Some(neigneig) = self.graph.get(n) {
                    for z in neigneig.iter() {
                        if neighbors.binary_search(z).is_ok() {
                            links = links + 1;
                        }
                    }
                }
            }
            (links as f64) / ((neighbors.len() as f64)  * (neighbors.len() as f64 - 1.0))
        }).collect::<Vec<_>>();
        local_coeffs.iter().fold(0., |x, y| x + y) / local_coeffs.len() as f64
    }

    pub fn indegree_dist(&self, n_procs: usize) -> Vec<usize> {
        if self.graph.is_empty() {
            return vec![0];
        }

        let n_byzantine = self.n_byzantine.unwrap();

        let mut ind = vec![0; n_procs];
        for (_, neigh) in self.graph.iter() {
            for i in neigh.iter() {
                if *i >= n_byzantine {
                    ind[i - n_byzantine] += 1;
                }
            }
        }
        ind.sort();
        ind
    }

    pub fn mean_path_length(&self, n_procs: usize) -> f64 {
        if self.graph.is_empty() {
            return 0.;
        }

        let n_byzantine = self.n_byzantine.unwrap();

        let mut rng = thread_rng();
        let roots = (0..32).map(|_| rng.gen_range(0, n_procs) + n_byzantine)
            .collect::<Vec<_>>();
        let avgdist = roots.par_iter().map(|root| {
                let mut dmap = HashMap::new();
                dmap.insert(*root, 0);
                let mut prev = HashSet::new();
                prev.insert(*root);
                let mut bfs_dist = 1;
                while !prev.is_empty() {
                    let mut next = HashSet::new();
                    for n in prev.iter() {
                        if let Some(nnl) = self.graph.get(n) {
                            for nn in nnl.iter() {
                                if *nn >= n_byzantine {
                                    if !dmap.contains_key(nn) {
                                        next.insert(*nn);
                                    }
                                }
                            }
                        } else {
                            println!("No neighbors: {}", n);
                        }
                    }
                    for n in next.iter() {
                        dmap.insert(*n, bfs_dist);
                    }
                    bfs_dist += 1;
                    prev = next;
                }
                dmap.iter().fold(0, |a, (_, b)| a + b) as f64 / dmap.len() as f64
            }).collect::<Vec<_>>();

        avgdist.iter().fold(0., |x, y| x + y) / avgdist.len() as f64
    }
}
