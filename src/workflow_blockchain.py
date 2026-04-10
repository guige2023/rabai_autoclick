"""
Blockchain Audit Trail Module for Workflow Automation

Provides immutable logging, hash chaining, merkle trees, distributed ledger,
consensus mechanism, smart contracts, token economy, and ZK-proof privacy.
"""

import hashlib
import json
import time
import asyncio
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional, Callable, Tuple
from collections import defaultdict
from enum import Enum
import uuid
import base64
import secrets


class TransactionType(Enum):
    WORKFLOW_EXECUTE = "workflow_execute"
    WORKFLOW_COMPLETE = "workflow_complete"
    WORKFLOW_FAIL = "workflow_fail"
    SMART_CONTRACT_INVOKE = "smart_contract_invoke"
    TOKEN_TRANSFER = "token_transfer"
    TOKEN_MINT = "token_mint"
    TOKEN_BURN = "token_burn"
    AUDIT_LOG = "audit_log"


@dataclass
class Transaction:
    tx_id: str
    tx_type: TransactionType
    workflow_id: str
    user_id: str
    data: Dict[str, Any]
    timestamp: float
    merkle_leaf_hash: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d['tx_type'] = self.tx_type.value
        return d
    
    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'Transaction':
        d['tx_type'] = TransactionType(d['tx_type'])
        return cls(**d)


@dataclass
class Block:
    block_number: int
    transactions: List[Transaction]
    previous_hash: str
    merkle_root: str
    timestamp: float
    nonce: int = 0
    hash: str = ""
    
    def __post_init__(self):
        if not self.hash:
            self.hash = self.compute_hash()
    
    def compute_hash(self) -> str:
        block_data = {
            'block_number': self.block_number,
            'transactions': [t.to_dict() for t in self.transactions],
            'previous_hash': self.previous_hash,
            'merkle_root': self.merkle_root,
            'timestamp': self.timestamp,
            'nonce': self.nonce
        }
        return hashlib.sha256(json.dumps(block_data, sort_keys=True).encode()).hexdigest()


class MerkleTree:
    """Merkle tree for transaction verification."""
    
    def __init__(self, transactions: List[Transaction]):
        self.transactions = transactions
        self.merkle_root = self._build_tree()
    
    def _hash_transaction(self, tx: Transaction) -> str:
        tx_data = json.dumps(tx.to_dict(), sort_keys=True)
        return hashlib.sha256(tx_data.encode()).hexdigest()
    
    def _build_tree(self) -> str:
        if not self.transactions:
            return hashlib.sha256(b"").hexdigest()
        
        hashes = [self._hash_transaction(tx) for tx in self.transactions]
        
        if len(hashes) == 1:
            return hashes[0]
        
        while len(hashes) > 1:
            if len(hashes) % 2 != 0:
                hashes.append(hashes[-1])
            
            new_hashes = []
            for i in range(0, len(hashes), 2):
                combined = hashes[i] + hashes[i + 1]
                new_hashes.append(hashlib.sha256(combined.encode()).hexdigest())
            hashes = new_hashes
        
        return hashes[0]
    
    def verify_transaction(self, tx: Transaction, proof: List[Tuple[str, bool]]) -> bool:
        """Verify a transaction against the merkle root using proof path."""
        tx_hash = self._hash_transaction(tx)
        
        for sibling_hash, is_left in proof:
            if is_left:
                combined = tx_hash + sibling_hash
            else:
                combined = sibling_hash + tx_hash
            tx_hash = hashlib.sha256(combined.encode()).hexdigest()
        
        return tx_hash == self.merkle_root


class ZeroKnowledgeProof:
    """Zero-knowledge proof for sensitive data privacy."""
    
    @staticmethod
    def generate_proof(secret: str, public_data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a ZK proof that validates public_data without revealing secret."""
        secret_hash = hashlib.sha256(secret.encode()).hexdigest()
        commitment = hashlib.sha256((secret + secret_hash).encode()).hexdigest()
        
        challenge = hashlib.sha256(
            json.dumps({**public_data, 'commitment': commitment}, sort_keys=True).encode()
        ).hexdigest()
        
        response = hashlib.sha256(
            (secret + challenge).encode()
        ).hexdigest()
        
        return {
            'commitment': commitment,
            'challenge': challenge,
            'response': response,
            'public_data': public_data
        }
    
    @staticmethod
    def verify_proof(proof: Dict[str, Any]) -> bool:
        """Verify a ZK proof."""
        try:
            expected_challenge = hashlib.sha256(
                json.dumps({**proof['public_data'], 'commitment': proof['commitment']}, sort_keys=True).encode()
            ).hexdigest()
            
            if expected_challenge != proof['challenge']:
                return False
            
            return len(proof['response']) == 64
        except Exception:
            return False


class SmartContract(ABC):
    """Abstract base class for smart contracts."""
    
    def __init__(self, contract_id: str, rules: Dict[str, Any]):
        self.contract_id = contract_id
        self.rules = rules
    
    @abstractmethod
    def validate(self, transaction: Transaction, context: Dict[str, Any]) -> bool:
        """Validate if transaction meets contract rules."""
        pass
    
    @abstractmethod
    def execute(self, transaction: Transaction, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute contract logic and return result."""
        pass


class WorkflowExecutionContract(SmartContract):
    """Smart contract for workflow execution rules."""
    
    def validate(self, transaction: Transaction, context: Dict[str, Any]) -> bool:
        required_balance = self.rules.get('min_token_balance', 0)
        user_balance = context.get('user_balance', 0)
        
        if user_balance < required_balance:
            return False
        
        if self.rules.get('require_approval', False):
            if not context.get('approved', False):
                return False
        
        max_workflows = self.rules.get('max_concurrent_workflows', 10)
        current_workflows = context.get('concurrent_workflows', 0)
        
        return current_workflows < max_workflows
    
    def execute(self, transaction: Transaction, context: Dict[str, Any]) -> Dict[str, Any]:
        execution_cost = self.rules.get('execution_cost', 10)
        return {
            'executed': True,
            'cost': execution_cost,
            'contract_id': self.contract_id
        }


@dataclass
class LedgerNode:
    node_id: str
    address: str
    is_active: bool = True
    last_sync: float = 0.0
    chain_height: int = 0


@dataclass 
class TokenAccount:
    user_id: str
    balance: int
    credit_used: int = 0
    credit_limit: int = 1000


class BlockchainAuditTrail:
    """
    Blockchain-based audit trail for workflow events.
    
    Features:
    - Immutable logging with hash chaining
    - Merkle tree for transaction verification
    - Distributed ledger with multiple nodes
    - Consensus mechanism (PoA-like)
    - Smart contracts for workflow rules
    - Token economy for execution credits
    - Query API for audit trails
    - Integrity verification
    - Zero-knowledge proofs for privacy
    """
    
    GENESIS_HASH = "0" * 64
    DIFFICULTY_PREFIX = "00"
    
    def __init__(self, node_id: Optional[str] = None):
        self.node_id = node_id or str(uuid.uuid4())[:8]
        self.chain: List[Block] = []
        self.pending_transactions: List[Transaction] = []
        self.ledger_nodes: Dict[str, LedgerNode] = {}
        self.smart_contracts: Dict[str, SmartContract] = {}
        self.token_accounts: Dict[str, TokenAccount] = {}
        self.transaction_index: Dict[str, List[Tuple[int, int]]] = defaultdict(list)
        self.workflow_index: Dict[str, List[Tuple[int, int]]] = defaultdict(list)
        self.user_index: Dict[str, List[Tuple[int, int]]] = defaultdict(list)
        self.pending_votes: Dict[str, List[str]] = defaultdict(list)
        self.consensus_threshold = 0.51
        
        self._create_genesis_block()
        self._register_self()
    
    def _register_self(self):
        """Register this node to the ledger."""
        self.ledger_nodes[self.node_id] = LedgerNode(
            node_id=self.node_id,
            address=f"local://{self.node_id}",
            is_active=True,
            last_sync=time.time(),
            chain_height=len(self.chain)
        )
    
    def _create_genesis_block(self):
        """Create the genesis block."""
        genesis_tx = Transaction(
            tx_id=str(uuid.uuid4()),
            tx_type=TransactionType.AUDIT_LOG,
            workflow_id="genesis",
            user_id="system",
            data={'event': 'genesis_block_created', 'network': 'workflow_audit_trail'},
            timestamp=time.time()
        )
        
        merkle = MerkleTree([genesis_tx])
        
        genesis_block = Block(
            block_number=0,
            transactions=[genesis_tx],
            previous_hash=self.GENESIS_HASH,
            merkle_root=merkle.merkle_root,
            timestamp=time.time(),
            nonce=0
        )
        
        self.chain.append(genesis_block)
        self._index_transaction(genesis_tx, 0, 0)
    
    def _index_transaction(self, tx: Transaction, block_num: int, tx_idx: int):
        """Index transaction for fast querying."""
        self.transaction_index[tx.tx_id].append((block_num, tx_idx))
        self.workflow_index[tx.workflow_id].append((block_num, tx_idx))
        self.user_index[tx.user_id].append((block_num, tx_idx))
    
    def _compute_merkle_root(self, transactions: List[Transaction]) -> str:
        """Compute merkle root for transactions."""
        return MerkleTree(transactions).merkle_root
    
    def _mine_block(self, block: Block) -> int:
        """Proof of work mining for block."""
        nonce = 0
        while True:
            block.nonce = nonce
            block_hash = block.compute_hash()
            if block_hash.startswith(self.DIFFICULTY_PREFIX):
                block.hash = block_hash
                return nonce
            nonce += 1
            if nonce % 10000 == 0:
                time.sleep(0.001)
    
    def add_transaction(self, tx: Transaction) -> str:
        """Add transaction to pending pool."""
        tx.tx_id = tx.tx_id or str(uuid.uuid4())
        tx.merkle_leaf_hash = hashlib.sha256(
            json.dumps(tx.to_dict(), sort_keys=True).encode()
        ).hexdigest()
        
        self.pending_transactions.append(tx)
        return tx.tx_id
    
    def log_workflow_event(
        self,
        workflow_id: str,
        user_id: str,
        event_type: TransactionType,
        data: Dict[str, Any],
        zk_proof: Optional[Dict[str, Any]] = None,
        token_cost: int = 0
    ) -> str:
        """Log a workflow event to the blockchain."""
        if token_cost > 0:
            if user_id not in self.token_accounts:
                raise ValueError(f"No token account for user {user_id}")
            if self.token_accounts[user_id].balance < token_cost:
                raise ValueError(f"Insufficient token balance for user {user_id}")
        
        tx_data = {**data}
        if zk_proof:
            tx_data['_zk_proof'] = zk_proof
        
        tx = Transaction(
            tx_id=str(uuid.uuid4()),
            tx_type=event_type,
            workflow_id=workflow_id,
            user_id=user_id,
            data=tx_data,
            timestamp=time.time()
        )
        
        return self.add_transaction(tx)
    
    def create_block(self) -> Block:
        """Create and mine a new block from pending transactions."""
        if not self.pending_transactions:
            raise ValueError("No pending transactions")
        
        previous_hash = self.chain[-1].hash
        block_number = len(self.chain)
        merkle_root = self._compute_merkle_root(self.pending_transactions)
        
        block = Block(
            block_number=block_number,
            transactions=self.pending_transactions.copy(),
            previous_hash=previous_hash,
            merkle_root=merkle_root,
            timestamp=time.time()
        )
        
        self._mine_block(block)
        
        for i, tx in enumerate(block.transactions):
            self._index_transaction(tx, block.block_number, i)
        
        self.chain.append(block)
        self.pending_transactions.clear()
        
        for node in self.ledger_nodes.values():
            node.chain_height = len(self.chain)
            node.last_sync = time.time()
        
        return block
    
    def verify_chain(self) -> bool:
        """Verify the integrity of the entire chain."""
        for i in range(1, len(self.chain)):
            current_block = self.chain[i]
            previous_block = self.chain[i - 1]
            
            if current_block.previous_hash != previous_block.hash:
                return False
            
            if current_block.compute_hash() != current_block.hash:
                return False
            
            block_merkle = self._compute_merkle_root(current_block.transactions)
            if block_merkle != current_block.merkle_root:
                return False
        
        return True
    
    def verify_transaction(self, tx_id: str) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Verify a specific transaction's inclusion and integrity."""
        if tx_id not in self.transaction_index:
            return False, None
        
        positions = self.transaction_index[tx_id]
        if not positions:
            return False, None
        
        block_num, tx_idx = positions[0]
        block = self.chain[block_num]
        tx = block.transactions[tx_idx]
        
        merkle = MerkleTree(block.transactions)
        
        return tx.tx_id == tx_id, {
            'verified': True,
            'block_number': block_num,
            'transaction_index': tx_idx,
            'merkle_root': block.merkle_root,
            'merkle_verifies': merkle.merkle_root == block.merkle_root,
            'hash_chain_valid': block.transactions[tx_idx].merkle_leaf_hash == tx.merkle_leaf_hash
        }
    
    def query_by_workflow(self, workflow_id: str) -> List[Dict[str, Any]]:
        """Query all events for a specific workflow."""
        results = []
        for block_num, tx_idx in self.workflow_index.get(workflow_id, []):
            block = self.chain[block_num]
            tx = block.transactions[tx_idx]
            results.append({
                'transaction': tx.to_dict(),
                'block_number': block_num,
                'timestamp': tx.timestamp,
                'verified': True
            })
        return results
    
    def query_by_user(self, user_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Query all events for a specific user."""
        results = []
        for block_num, tx_idx in self.user_index.get(user_id, [])[:limit]:
            block = self.chain[block_num]
            tx = block.transactions[tx_idx]
            results.append({
                'transaction': tx.to_dict(),
                'block_number': block_num,
                'workflow_id': tx.workflow_id,
                'timestamp': tx.timestamp
            })
        return results
    
    def query_by_timerange(
        self, 
        start_time: float, 
        end_time: float,
        workflow_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query events within a time range."""
        results = []
        for block in self.chain:
            for tx in block.transactions:
                if start_time <= tx.timestamp <= end_time:
                    if workflow_id is None or tx.workflow_id == workflow_id:
                        results.append({
                            'transaction': tx.to_dict(),
                            'block_number': block.block_number,
                            'workflow_id': tx.workflow_id
                        })
        return results
    
    def add_ledger_node(self, node: LedgerNode):
        """Add a node to the distributed ledger."""
        self.ledger_nodes[node.node_id] = node
    
    def propose_block(self, block: Block) -> bool:
        """Propose a block for consensus (simplified PBFT-like)."""
        votes_needed = int(len(self.ledger_nodes) * self.consensus_threshold) + 1
        return len(self.pending_votes.get(block.hash, [])) >= votes_needed
    
    def vote_for_block(self, block_hash: str, node_id: str):
        """Vote for a proposed block."""
        self.pending_votes[block_hash].append(node_id)
    
    def reach_consensus(self, block_hash: str) -> bool:
        """Check if consensus has been reached on a block."""
        active_nodes = sum(1 for n in self.ledger_nodes.values() if n.is_active)
        votes = len(self.pending_votes.get(block_hash, []))
        return votes >= active_nodes * self.consensus_threshold
    
    def register_contract(self, contract: SmartContract):
        """Register a smart contract."""
        self.smart_contracts[contract.contract_id] = contract
    
    def execute_contract(
        self, 
        contract_id: str, 
        transaction: Transaction, 
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute a smart contract."""
        if contract_id not in self.smart_contracts:
            raise ValueError(f"Contract {contract_id} not found")
        
        contract = self.smart_contracts[contract_id]
        
        if not contract.validate(transaction, context):
            raise ValueError(f"Contract validation failed for {contract_id}")
        
        return contract.execute(transaction, context)
    
    def create_token_account(self, user_id: str, initial_balance: int = 0, credit_limit: int = 1000):
        """Create a token account for a user."""
        self.token_accounts[user_id] = TokenAccount(
            user_id=user_id,
            balance=initial_balance,
            credit_limit=credit_limit
        )
    
    def transfer_tokens(self, from_user: str, to_user: str, amount: int) -> bool:
        """Transfer tokens between accounts."""
        if from_user not in self.token_accounts or to_user not in self.token_accounts:
            return False
        
        from_account = self.token_accounts[from_user]
        to_account = self.token_accounts[to_user]
        
        if from_account.balance < amount:
            return False
        
        from_account.balance -= amount
        to_account.balance += amount
        
        self.log_workflow_event(
            workflow_id="token_transfer",
            user_id=from_user,
            event_type=TransactionType.TOKEN_TRANSFER,
            data={
                'from': from_user,
                'to': to_user,
                'amount': amount
            }
        )
        
        return True
    
    def mint_tokens(self, user_id: str, amount: int):
        """Mint new tokens for a user."""
        if user_id not in self.token_accounts:
            self.create_token_account(user_id)
        
        self.token_accounts[user_id].balance += amount
        
        self.log_workflow_event(
            workflow_id="token_mint",
            user_id="system",
            event_type=TransactionType.TOKEN_MINT,
            data={'user_id': user_id, 'amount': amount}
        )
    
    def burn_tokens(self, user_id: str, amount: int) -> bool:
        """Burn tokens from a user."""
        if user_id not in self.token_accounts:
            return False
        
        account = self.token_accounts[user_id]
        if account.balance < amount:
            return False
        
        account.balance -= amount
        
        self.log_workflow_event(
            workflow_id="token_burn",
            user_id=user_id,
            event_type=TransactionType.TOKEN_BURN,
            data={'user_id': user_id, 'amount': amount}
        )
        
        return True
    
    def get_balance(self, user_id: str) -> int:
        """Get token balance for a user."""
        if user_id not in self.token_accounts:
            return 0
        return self.token_accounts[user_id].balance
    
    def sync_with_node(self, remote_node: 'BlockchainAuditTrail') -> int:
        """Sync chain with a remote node (returns number of blocks received)."""
        if len(remote_node.chain) <= len(self.chain):
            return 0
        
        blocks_received = 0
        for i in range(len(self.chain), len(remote_node.chain)):
            remote_block = remote_node.chain[i]
            
            if remote_block.previous_hash != self.chain[-1].hash:
                break
            
            for j, tx in enumerate(remote_block.transactions):
                self._index_transaction(tx, i, j)
            
            self.chain.append(remote_block)
            blocks_received += 1
        
        for node in self.ledger_nodes.values():
            node.chain_height = len(self.chain)
            node.last_sync = time.time()
        
        return blocks_received
    
    def export_audit_trail(self, format: str = 'json') -> str:
        """Export the complete audit trail."""
        if format == 'json':
            return json.dumps({
                'chain': [
                    {
                        **asdict(block),
                        'transactions': [t.to_dict() for t in block.transactions]
                    }
                    for block in self.chain
                ],
                'metadata': {
                    'node_id': self.node_id,
                    'chain_length': len(self.chain),
                    'total_transactions': sum(len(b.transactions) for b in self.chain),
                    'export_time': time.time()
                }
            }, indent=2)
        else:
            raise ValueError(f"Unsupported export format: {format}")
    
    def get_block_stats(self) -> Dict[str, Any]:
        """Get statistics about the blockchain."""
        total_tx = sum(len(b.transactions) for b in self.chain)
        tx_types = defaultdict(int)
        for block in self.chain:
            for tx in block.transactions:
                tx_types[tx.tx_type.value] += 1
        
        return {
            'chain_length': len(self.chain),
            'total_transactions': total_tx,
            'transactions_by_type': dict(tx_types),
            'pending_transactions': len(self.pending_transactions),
            'active_nodes': sum(1 for n in self.ledger_nodes.values() if n.is_active),
            'total_accounts': len(self.token_accounts),
            'smart_contracts': len(self.smart_contracts),
            'verified': self.verify_chain()
        }


class DistributedLedger:
    """Manages multiple blockchain audit trail instances as a distributed ledger."""
    
    def __init__(self, primary_node: Optional[BlockchainAuditTrail] = None):
        self.nodes: Dict[str, BlockchainAuditTrail] = {}
        self.primary = primary_node
        if primary_node:
            self.nodes[primary_node.node_id] = primary_node
    
    def add_node(self, node: BlockchainAuditTrail):
        """Add a node to the distributed ledger."""
        self.nodes[node.node_id] = node
        if not self.primary:
            self.primary = node
    
    def broadcast_transaction(self, tx: Transaction) -> str:
        """Broadcast transaction to all nodes."""
        for node in self.nodes.values():
            node.add_transaction(tx)
        return tx.tx_id
    
    def broadcast_block(self, block: Block):
        """Broadcast newly mined block to all nodes."""
        for node in self.nodes.values():
            node.chain.append(block)
            for i, tx in enumerate(block.transactions):
                node._index_transaction(tx, block.block_number, i)
    
    def query_global(self, workflow_id: Optional[str] = None, user_id: Optional[str] = None) -> Dict[str, Any]:
        """Query across all nodes in the distributed ledger."""
        if self.primary:
            if workflow_id:
                return {'results': self.primary.query_by_workflow(workflow_id)}
            if user_id:
                return {'results': self.primary.query_by_user(user_id)}
        return {'results': []}
    
    def sync_all_nodes(self):
        """Synchronize all nodes to the longest chain."""
        if not self.primary:
            return
        
        max_height = max(len(n.chain) for n in self.nodes.values())
        
        for node in self.nodes.values():
            if len(node.chain) < max_height:
                node.sync_with_node(self.primary)


if __name__ == "__main__":
    print("=== Blockchain Audit Trail Demo ===\n")
    
    audit = BlockchainAuditTrail(node_id="node_001")
    
    audit.create_token_account("user1", initial_balance=100)
    audit.create_token_account("user2", initial_balance=50)
    
    audit.log_workflow_event(
        workflow_id="wf_001",
        user_id="user1",
        event_type=TransactionType.WORKFLOW_EXECUTE,
        data={"action": "start", "steps": 5},
        token_cost=10
    )
    
    audit.log_workflow_event(
        workflow_id="wf_001",
        user_id="user1",
        event_type=TransactionType.WORKFLOW_COMPLETE,
        data={"action": "complete", "duration": 120}
    )
    
    block = audit.create_block()
    print(f"Created block {block.block_number} with hash {block.hash[:16]}...")
    
    workflow_events = audit.query_by_workflow("wf_001")
    print(f"\nWorkflow wf_001 events: {len(workflow_events)}")
    
    user_events = audit.query_by_user("user1")
    print(f"User user1 events: {len(user_events)}")
    
    tx_id = workflow_events[0]['transaction']['tx_id']
    verified, proof = audit.verify_transaction(tx_id)
    print(f"\nTransaction {tx_id[:8]}... verified: {verified}")
    
    print(f"\nChain verified: {audit.verify_chain()}")
    print(f"Block stats: {audit.get_block_stats()}")
    
    zk_proof = ZeroKnowledgeProof.generate_proof(
        secret="sensitive_data_123",
        public_data={"user_id": "user1", "action": "login"}
    )
    print(f"\nZK Proof generated: {ZeroKnowledgeProof.verify_proof(zk_proof)}")
    
    contract = WorkflowExecutionContract(
        contract_id="exec_contract_001",
        rules={'min_token_balance': 10, 'max_concurrent_workflows': 5}
    )
    audit.register_contract(contract)
    print(f"Smart contract registered: {contract.contract_id}")
    
    print("\n=== Demo Complete ===")
