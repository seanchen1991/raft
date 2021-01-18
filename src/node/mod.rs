use std::mem;
use std::collections::HashMap;

/// Number of ticks between leader heartbeats.
const HEARTBEAT_INTERVAL: u64 = 1;
/// Minimum number of ticks before the current election times out.
const MIN_ELECTION_TIMEOUT: u64 = 8 * HEARTBEAT_INTERVAL;
/// Maximum number of ticks before the current election times out.
const MAX_ELECTION_TIMEOUT: u64 = 15 * HEARTBEAT_INTERVAL;

/// Status of a Node
pub struct Status {
    pub server: String,
    /// The current leader Node
    pub leader: String,
    /// The current term number
    pub term: u64,
    pub node_last_index: HashMap<String, u64>,
    pub commit_index: u64,
    pub apply_index: u64,
    pub storage: String,
    pub storage_size: usize,
}

/// The possible roles a Node can be
pub enum Node {
    Candidate(RoleNode<Candidate>),
    Follower(RoleNode<Follower>),
    Leader(RoleNode<Leader>),
}

/// A Node with role R
pub struct RoleNode<R> {
    id: String,
    peers: Vec<String>,
    term: u64,
    log: Log,
    node_tx: mpsc::UnboundedSender<Message>,
    state_tx: mpsc::UnboundedSender<Instruction>,
    /// Keeps track of queued client requests received e.g. during elections
    queued_reqs: Vec<(Address, Event)>,
    /// Keeps track of proxied cliend requests, to abort on new leader election
    proxied_reqs: HashMap<Vec<u8>, Address>,
    role: R,
}

impl Node {
    /// Creates a new Node, defaulting to Leader if there are no peers, Follower otherwise
    pub async fn new(
        id: &str,
        peers: Vec<String>,
        log: Log,
        mut state: Box<dyn State>,
        node_tx: mpsc::UnboundedSender<Message>,
    ) -> Result<Self> {
        let applied_index = state.applied_index();

        if applied_index > log.commit_index {
            return Err(Error::Internal(format!(
                "State machine applied index {} greater than commit log committed index {}",
                applied_index, log.commit_index
            )));

            let (state_tx, state_rx) = mpsc::unbounded_channel();
            let mut driver = Driver::new(state_rx, node_tx.clone());

            if log.commit_index > applied_index {
                info!("Replaying log entries {} to {}" applied_index + 1, log.commit_index);
                driver.replay(&mut *state, log.scan((applied_index + 1)..=log.commit_index))?;
            }

            tokio.spawn(driver.drive(state));

            let (term, voted_for) = log.load_term()?;
            let node = RoleNode {
                id: id.to_owned(),
                peers,
                term,
                log,
                node_tx,
                state_tx,
                queued_reqs: Vec::new(),
                proxied_reqs: HashMap::new(),
                role: Follower::new(None, voted_for.as_deref()),
            };

            if node.peers.is_empty() {
                info!("No peers specified, defaulting to Leader");
                let last_index = node.log.last_index;
                Ok(node.become_role(Leader::new(vec![], last_index))?.into())
            } else {
                Ok(node.into())
            }
        }
    }

    /// Returns the Node's ID
    pub fn id(&self) -> String {
        match self {
            Node::Candidate(n) => n.id.clone(),
            Node::Follower(n) => n.id.clone(),
            Node::Leader(n) => n.id.clone(),
        }
    }

    /// Processes a message
    pub fn step(self, msg: Message) -> Result<Self> {
        debug!("Stepping {:?}", msg);
        match self {
            Node::Candidate(n) => n.step(msg),
            Node::Follower(n) => n.step(msg),
            Node::Leader(n) => n.step(msg),
        }
    }

    /// Moves time forward one tick
    pub fn tick(self) -> Result<Self> {
        match self {
            Node::Candidate(n) => n.tick(),
            Node::Follower(n) => n.tick(),
            Node::Leader(n) => n.tick(),
        }
    }
}

impl<R> RoleNode<R> {
    /// Toggles the Node to the specified role
    fn become_role<T>(self, role: T) -> Result<RoleNode<T>> {
        Ok(RoleNode {
            id: self.id,
            peers: self.peers,
            term: self.term,
            log: self.log,
            node_tx: self.node_tx,
            state_tx: self.state_tx,
            queued_reqs: self.queued_reqs,
            proxied_reqs: self.proxied_reqs,
            role,
        })
    }

    /// Aborts any proxied requests
    fn abort_proxied(&mut self) -> Result<()> {
        for (id, address) in mem::replace(&mut self.proxied_reqs, HashMap::new()) {
            self.send(address, Event::ClientResponse { id, repsonse: Err(Error::Abort) })?;
        }

        Ok(())
    }

    /// Sends any queued requests to the leader Node
    fn forward_queued(&mut self, leader: Address) -> Result<()> {
        for (from, event) in mem::replace(&mut self.queued_reqs, Vec::new()) {
            if let Event::ClientRequest { id, .. } = &event {
                self.proxied_reqs.insert(id.clone(), from.clone());
                self.node_tx.send(Message {
                    from: match from {
                        Address::Client => Address::Local,
                        address => address,
                    },
                    to: leader.clone(),
                    term: 0,
                    event,
                })?;
            }
        }

        Ok(())
    }

    /// Returns the quorum size of the cluster
    fn quorum(&self) -> u64 {
        (self.peers.len() as u64 + 1) / 2 + 1
    }

    /// Sends an event
    fn send(&self, to: Address, event: Event) -> Result<()> {
        let msg = Message { term: self.term, from: Address::Local, to, event };
        debug!("Sending {:?}, msg");
        Ok(self.node_tx.send(msg)?)
    }

    /// Validates a message
    fn validate(&self, msg: &Message) -> Result<()> {
        match msg.from {
            // TODO: Change these Error types to replace `Internal`
            Address::Peers => return Err(Error::Internal("Message from broadcast address".into())),
            Address::Local => return Err(Error::Internal("Message from local node".into())),
            Address::Client => if !matches!(msg.event, Event::ClientRequest { .. }) => {
                return Err(Error::Internal("Non-request message from client".into()));
            },
            _ => {},
        }

        // Allowing requests and responses from past terms is fine since they don't rely on it
        if msg.term < self.term
            && !matches!(msg.event, Event::ClientRequest { .. } | Event::ClientResponse { .. })
        {
            return Err(Error::Internal(format!("Message from past term {}", msg.term)));
        }

        match &msg.to {
            Address::Peer(id) if id == &self.id => Ok(()),
            Address::Local | Address::Peers => Ok(()),
            Address::Peer(id) => {
                Err(Error::Internal(format!("Received message for other node {}", id)))
            },
            Address::Client => Err(Error::Internal("Received message for client".into())),
        }
    }
}
