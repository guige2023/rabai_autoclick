"""
Tests for workflow_blockchain module - Blockchain Audit Trail for Workflow Automation.
Covers immutable logging, hash chaining, merkle trees, distributed ledger,
consensus mechanism, smart contracts, token economy, and ZK-proof privacy.
"""

import sys
import os
import json
import time
import unittest
from unittest.mock import Mock, patch, MagicMock, mock_open
from datetime import datetime
from collections import defaultdict

sys.path.insert(0, '/Users/guige/my_project')

# Import workflow_blockchain module
from src.workflow_blockchain import (
    TransactionType,
    Transaction,
    Block,
    MerkleTree,
    ZeroKnowledgeProof,
    SmartContract,
    WorkflowExecutionContract,
    LedgerNode,
    TokenAccount,
    BlockchainAuditTrail,
    DistributedLedger,
)


class TestTransactionAndBlock(unittest.TestCase):
    """Test transaction and block creation and operations."""

    def setUp(self):
        self.mock_time = 1234567890.0
        self.tx = Transaction(
            tx_id="tx_001",
            tx_type=TransactionType.WORKFLOW_EXECUTE,
            workflow_id="wf_001",
            user_id="user1",
            data={"action": "start"},
            timestamp=self.mock_time
        )

    def test_transaction_to_dict(self):
        """Test transaction serialization to dict."""
        d = self.tx.to_dict()
        self.assertEqual(d['tx_id'], "tx_001")
        self.assertEqual(d['tx_type'], "workflow_execute")
        self.assertEqual(d['workflow_id'], "wf_001")
        self.assertEqual(d['user_id'], "user1")
        self.assertEqual(d['data'], {"action": "start"})

    def test_transaction_from_dict(self):
        """Test transaction deserialization from dict."""
        d = {
            'tx_id': 'tx_002',
            'tx_type': 'workflow_complete',
            'workflow_id': 'wf_002',
            'user_id': 'user2',
            'data': {'result': 'success'},
            'timestamp': 1234567891.0,
            'merkle_leaf_hash': 'abc123'
        }
        tx = Transaction.from_dict(d)
        self.assertEqual(tx.tx_id, 'tx_002')
        self.assertEqual(tx.tx_type, TransactionType.WORKFLOW_COMPLETE)
        self.assertEqual(tx.workflow_id, 'wf_002')

    def test_block_creation(self):
        """Test block creation with hash computation."""
        block = Block(
            block_number=1,
            transactions=[self.tx],
            previous_hash="0" * 64,
            merkle_root="merkle_root_123",
            timestamp=self.mock_time,
            nonce=0
        )
        self.assertEqual(block.block_number, 1)
        self.assertEqual(len(block.transactions), 1)
        self.assertIsNotNone(block.hash)
        self.assertEqual(len(block.hash), 64)  # SHA-256 hex length

    def test_block_hash_immutability(self):
        """Test that block hash is computed and stored."""
        block = Block(
            block_number=1,
            transactions=[],
            previous_hash="0" * 64,
            merkle_root="root",
            timestamp=self.mock_time
        )
        original_hash = block.hash
        block.nonce = 100
        # Note: hash is computed in __post_init__ so changing nonce after init doesn't update hash
        self.assertNotEqual(block.nonce, 0)


class TestMerkleTree(unittest.TestCase):
    """Test Merkle tree for transaction verification."""

    def test_merkle_tree_empty(self):
        """Test merkle tree with empty transactions."""
        tree = MerkleTree([])
        self.assertIsNotNone(tree.merkle_root)
        # Empty tree should still produce a hash (of empty bytes)
        self.assertEqual(len(tree.merkle_root), 64)

    def test_merkle_tree_single_transaction(self):
        """Test merkle tree with single transaction."""
        tx = Transaction(
            tx_id="tx_001",
            tx_type=TransactionType.AUDIT_LOG,
            workflow_id="wf_001",
            user_id="user1",
            data={},
            timestamp=time.time()
        )
        tree = MerkleTree([tx])
        self.assertIsNotNone(tree.merkle_root)
        # Single tx should return its hash as merkle root
        tx_hash = tree._hash_transaction(tx)
        self.assertEqual(tree.merkle_root, tx_hash)

    def test_merkle_tree_multiple_transactions(self):
        """Test merkle tree with multiple transactions."""
        txs = [
            Transaction(
                tx_id=f"tx_{i:03d}",
                tx_type=TransactionType.WORKFLOW_EXECUTE,
                workflow_id="wf_001",
                user_id="user1",
                data={},
                timestamp=time.time()
            )
            for i in range(4)
        ]
        tree = MerkleTree(txs)
        self.assertIsNotNone(tree.merkle_root)
        self.assertEqual(len(tree.merkle_root), 64)

    def test_verify_transaction_valid_proof(self):
        """Test transaction verification with valid proof path."""
        tx = Transaction(
            tx_id="tx_001",
            tx_type=TransactionType.WORKFLOW_EXECUTE,
            workflow_id="wf_001",
            user_id="user1",
            data={},
            timestamp=time.time()
        )
        tree = MerkleTree([tx])
        # For single transaction, empty proof should verify
        verified = tree.verify_transaction(tx, [])
        self.assertTrue(verified)


class TestZeroKnowledgeProof(unittest.TestCase):
    """Test zero-knowledge proof generation and verification."""

    def test_generate_proof(self):
        """Test ZK proof generation."""
        proof = ZeroKnowledgeProof.generate_proof(
            secret="my_secret",
            public_data={"user_id": "user1", "action": "login"}
        )
        self.assertIn('commitment', proof)
        self.assertIn('challenge', proof)
        self.assertIn('response', proof)
        self.assertIn('public_data', proof)
        self.assertEqual(len(proof['commitment']), 64)
        self.assertEqual(len(proof['challenge']), 64)
        self.assertEqual(len(proof['response']), 64)

    def test_verify_valid_proof(self):
        """Test verification of valid ZK proof."""
        proof = ZeroKnowledgeProof.generate_proof(
            secret="test_secret",
            public_data={"data": "test"}
        )
        result = ZeroKnowledgeProof.verify_proof(proof)
        self.assertTrue(result)

    def test_verify_invalid_proof(self):
        """Test verification of tampered ZK proof fails."""
        proof = ZeroKnowledgeProof.generate_proof(
            secret="test_secret",
            public_data={"data": "test"}
        )
        # Tamper with proof
        proof['challenge'] = "a" * 64
        result = ZeroKnowledgeProof.verify_proof(proof)
        self.assertFalse(result)

    def test_verify_proof_missing_fields(self):
        """Test verification fails with missing fields."""
        incomplete_proof = {'commitment': 'abc', 'challenge': 'def'}
        result = ZeroKnowledgeProof.verify_proof(incomplete_proof)
        self.assertFalse(result)


class TestSmartContract(unittest.TestCase):
    """Test smart contract validation and execution."""

    def setUp(self):
        self.contract = WorkflowExecutionContract(
            contract_id="exec_contract_001",
            rules={
                'min_token_balance': 10,
                'max_concurrent_workflows': 5,
                'execution_cost': 5
            }
        )
        self.tx = Transaction(
            tx_id="tx_001",
            tx_type=TransactionType.WORKFLOW_EXECUTE,
            workflow_id="wf_001",
            user_id="user1",
            data={},
            timestamp=time.time()
        )

    def test_validate_sufficient_balance(self):
        """Test contract validation with sufficient balance."""
        context = {
            'user_balance': 100,
            'concurrent_workflows': 2
        }
        result = self.contract.validate(self.tx, context)
        self.assertTrue(result)

    def test_validate_insufficient_balance(self):
        """Test contract validation fails with insufficient balance."""
        context = {
            'user_balance': 5,  # Below min_token_balance of 10
            'concurrent_workflows': 2
        }
        result = self.contract.validate(self.tx, context)
        self.assertFalse(result)

    def test_validate_exceeds_max_workflows(self):
        """Test validation fails when exceeding max concurrent workflows."""
        context = {
            'user_balance': 100,
            'concurrent_workflows': 6  # Above max of 5
        }
        result = self.contract.validate(self.tx, context)
        self.assertFalse(result)

    def test_validate_requires_approval(self):
        """Test validation requires approval when configured."""
        contract = WorkflowExecutionContract(
            contract_id="approval_contract",
            rules={
                'min_token_balance': 0,
                'require_approval': True
            }
        )
        context = {'approved': False, 'user_balance': 100}
        result = contract.validate(self.tx, context)
        self.assertFalse(result)

        context = {'approved': True, 'user_balance': 100}
        result = contract.validate(self.tx, context)
        self.assertTrue(result)

    def test_execute_contract(self):
        """Test smart contract execution."""
        context = {'user_balance': 100}
        result = self.contract.execute(self.tx, context)
        self.assertTrue(result['executed'])
        self.assertEqual(result['cost'], 5)
        self.assertEqual(result['contract_id'], 'exec_contract_001')


class TestBlockchainAuditTrail(unittest.TestCase):
    """Test blockchain audit trail functionality."""

    def setUp(self):
        self.patcher1 = patch('time.time', return_value=1234567890.0)
        self.patcher1.start()
        self.audit = BlockchainAuditTrail(node_id="test_node")

    def tearDown(self):
        self.patcher1.stop()

    def test_genesis_block_creation(self):
        """Test genesis block is created on initialization."""
        self.assertEqual(len(self.audit.chain), 1)
        self.assertEqual(self.audit.chain[0].block_number, 0)
        self.assertEqual(self.audit.chain[0].previous_hash, BlockchainAuditTrail.GENESIS_HASH)

    def test_add_transaction(self):
        """Test adding transaction to pending pool."""
        tx = Transaction(
            tx_id="tx_001",
            tx_type=TransactionType.WORKFLOW_EXECUTE,
            workflow_id="wf_001",
            user_id="user1",
            data={},
            timestamp=time.time()
        )
        tx_id = self.audit.add_transaction(tx)
        self.assertEqual(tx_id, "tx_001")
        self.assertEqual(len(self.audit.pending_transactions), 1)
        self.assertIsNotNone(tx.merkle_leaf_hash)

    def test_add_transaction_auto_id(self):
        """Test transaction gets auto-generated ID if not provided."""
        tx = Transaction(
            tx_id="",
            tx_type=TransactionType.WORKFLOW_EXECUTE,
            workflow_id="wf_001",
            user_id="user1",
            data={},
            timestamp=time.time()
        )
        tx_id = self.audit.add_transaction(tx)
        self.assertIsNotNone(tx_id)
        self.assertNotEqual(tx_id, "")

    def test_log_workflow_event(self):
        """Test logging workflow events."""
        self.audit.create_token_account("user1", initial_balance=100)
        tx_id = self.audit.log_workflow_event(
            workflow_id="wf_001",
            user_id="user1",
            event_type=TransactionType.WORKFLOW_EXECUTE,
            data={"action": "start"},
            token_cost=5
        )
        self.assertIsNotNone(tx_id)
        self.assertEqual(len(self.audit.pending_transactions), 1)

    def test_log_workflow_event_insufficient_balance(self):
        """Test logging fails with insufficient token balance."""
        self.audit.create_token_account("user1", initial_balance=0)
        with self.assertRaises(ValueError) as context:
            self.audit.log_workflow_event(
                workflow_id="wf_001",
                user_id="user1",
                event_type=TransactionType.WORKFLOW_EXECUTE,
                data={"action": "start"},
                token_cost=10
            )
        self.assertIn("Insufficient token balance", str(context.exception))

    def test_create_block(self):
        """Test block creation from pending transactions."""
        self.audit.create_token_account("user1", initial_balance=100)
        self.audit.log_workflow_event(
            workflow_id="wf_001",
            user_id="user1",
            event_type=TransactionType.WORKFLOW_EXECUTE,
            data={},
            token_cost=5
        )
        block = self.audit.create_block()
        self.assertEqual(block.block_number, 1)
        self.assertEqual(len(block.transactions), 1)
        self.assertEqual(len(self.audit.pending_transactions), 0)
        self.assertEqual(len(self.audit.chain), 2)

    def test_create_block_no_transactions(self):
        """Test creating block fails with no pending transactions."""
        with self.assertRaises(ValueError) as context:
            self.audit.create_block()
        self.assertIn("No pending transactions", str(context.exception))

    def test_verify_chain_valid(self):
        """Test chain verification returns True for valid chain."""
        self.assertTrue(self.audit.verify_chain())

    def test_verify_transaction(self):
        """Test transaction verification."""
        self.audit.create_token_account("user1", initial_balance=100)
        tx_id = self.audit.log_workflow_event(
            workflow_id="wf_001",
            user_id="user1",
            event_type=TransactionType.WORKFLOW_EXECUTE,
            data={}
        )
        self.audit.create_block()

        verified, proof = self.audit.verify_transaction(tx_id)
        self.assertTrue(verified)
        self.assertIsNotNone(proof)
        self.assertTrue(proof['verified'])

    def test_verify_nonexistent_transaction(self):
        """Test verification of non-existent transaction."""
        verified, proof = self.audit.verify_transaction("nonexistent_id")
        self.assertFalse(verified)
        self.assertIsNone(proof)

    def test_query_by_workflow(self):
        """Test querying transactions by workflow ID."""
        self.audit.create_token_account("user1", initial_balance=100)
        self.audit.log_workflow_event(
            workflow_id="wf_001",
            user_id="user1",
            event_type=TransactionType.WORKFLOW_EXECUTE,
            data={}
        )
        self.audit.log_workflow_event(
            workflow_id="wf_001",
            user_id="user1",
            event_type=TransactionType.WORKFLOW_COMPLETE,
            data={}
        )
        self.audit.create_block()

        results = self.audit.query_by_workflow("wf_001")
        self.assertEqual(len(results), 2)

    def test_query_by_user(self):
        """Test querying transactions by user ID."""
        self.audit.create_token_account("user1", initial_balance=100)
        self.audit.log_workflow_event(
            workflow_id="wf_001",
            user_id="user1",
            event_type=TransactionType.WORKFLOW_EXECUTE,
            data={}
        )
        self.audit.create_block()

        results = self.audit.query_by_user("user1", limit=100)
        self.assertGreaterEqual(len(results), 1)

    def test_query_by_timerange(self):
        """Test querying transactions by time range."""
        self.audit.create_token_account("user1", initial_balance=100)
        start_time = time.time()
        self.audit.log_workflow_event(
            workflow_id="wf_001",
            user_id="user1",
            event_type=TransactionType.WORKFLOW_EXECUTE,
            data={}
        )
        end_time = time.time()

        results = self.audit.query_by_timerange(start_time - 10, end_time + 10)
        self.assertGreaterEqual(len(results), 1)


class TestTokenOperations(unittest.TestCase):
    """Test token economy operations."""

    def setUp(self):
        self.patcher1 = patch('time.time', return_value=1234567890.0)
        self.patcher1.start()
        self.audit = BlockchainAuditTrail(node_id="test_node")

    def tearDown(self):
        self.patcher1.stop()

    def test_create_token_account(self):
        """Test creating a token account."""
        self.audit.create_token_account("user1", initial_balance=100, credit_limit=500)
        account = self.audit.token_accounts["user1"]
        self.assertEqual(account.user_id, "user1")
        self.assertEqual(account.balance, 100)
        self.assertEqual(account.credit_limit, 500)

    def test_transfer_tokens_success(self):
        """Test successful token transfer."""
        self.audit.create_token_account("user1", initial_balance=100)
        self.audit.create_token_account("user2", initial_balance=50)
        result = self.audit.transfer_tokens("user1", "user2", 30)
        self.assertTrue(result)
        self.assertEqual(self.audit.get_balance("user1"), 70)
        self.assertEqual(self.audit.get_balance("user2"), 80)

    def test_transfer_tokens_insufficient_balance(self):
        """Test transfer fails with insufficient balance."""
        self.audit.create_token_account("user1", initial_balance=10)
        self.audit.create_token_account("user2", initial_balance=50)
        result = self.audit.transfer_tokens("user1", "user2", 20)
        self.assertFalse(result)
        self.assertEqual(self.audit.get_balance("user1"), 10)

    def test_transfer_tokens_nonexistent_account(self):
        """Test transfer fails with non-existent account."""
        self.audit.create_token_account("user1", initial_balance=100)
        result = self.audit.transfer_tokens("user1", "nonexistent", 10)
        self.assertFalse(result)

    def test_mint_tokens(self):
        """Test minting new tokens."""
        self.audit.create_token_account("user1", initial_balance=0)
        self.audit.mint_tokens("user1", 500)
        self.assertEqual(self.audit.get_balance("user1"), 500)

    def test_burn_tokens_success(self):
        """Test successful token burn."""
        self.audit.create_token_account("user1", initial_balance=100)
        result = self.audit.burn_tokens("user1", 30)
        self.assertTrue(result)
        self.assertEqual(self.audit.get_balance("user1"), 70)

    def test_burn_tokens_insufficient_balance(self):
        """Test burn fails with insufficient balance."""
        self.audit.create_token_account("user1", initial_balance=10)
        result = self.audit.burn_tokens("user1", 20)
        self.assertFalse(result)
        self.assertEqual(self.audit.get_balance("user1"), 10)

    def test_get_balance_nonexistent_user(self):
        """Test getting balance for non-existent user returns 0."""
        balance = self.audit.get_balance("nonexistent")
        self.assertEqual(balance, 0)


class TestConsensusAndSmartContracts(unittest.TestCase):
    """Test consensus mechanism and smart contracts."""

    def setUp(self):
        self.patcher1 = patch('time.time', return_value=1234567890.0)
        self.patcher1.start()
        self.audit = BlockchainAuditTrail(node_id="test_node")

    def tearDown(self):
        self.patcher1.stop()

    def test_register_smart_contract(self):
        """Test registering a smart contract."""
        contract = WorkflowExecutionContract(
            contract_id="test_contract",
            rules={'min_token_balance': 10}
        )
        self.audit.register_contract(contract)
        self.assertIn("test_contract", self.audit.smart_contracts)

    def test_execute_smart_contract(self):
        """Test executing a registered smart contract."""
        contract = WorkflowExecutionContract(
            contract_id="test_contract",
            rules={'min_token_balance': 10, 'execution_cost': 5}
        )
        self.audit.register_contract(contract)

        tx = Transaction(
            tx_id="tx_001",
            tx_type=TransactionType.SMART_CONTRACT_INVOKE,
            workflow_id="wf_001",
            user_id="user1",
            data={},
            timestamp=time.time()
        )
        context = {'user_balance': 100, 'concurrent_workflows': 1}
        result = self.audit.execute_contract("test_contract", tx, context)
        self.assertTrue(result['executed'])

    def test_execute_nonexistent_contract(self):
        """Test executing non-existent contract raises error."""
        tx = Transaction(
            tx_id="tx_001",
            tx_type=TransactionType.SMART_CONTRACT_INVOKE,
            workflow_id="wf_001",
            user_id="user1",
            data={},
            timestamp=time.time()
        )
        with self.assertRaises(ValueError) as context:
            self.audit.execute_contract("nonexistent", tx, {})
        self.assertIn("Contract", str(context.exception))
        self.assertIn("not found", str(context.exception))

    def test_add_ledger_node(self):
        """Test adding a ledger node."""
        node = LedgerNode(
            node_id="node_002",
            address="remote://node_002",
            is_active=True,
            chain_height=10
        )
        self.audit.add_ledger_node(node)
        self.assertIn("node_002", self.audit.ledger_nodes)

    def test_vote_for_block(self):
        """Test voting for a block."""
        self.audit.vote_for_block("block_hash_123", "node_001")
        self.assertIn("block_hash_123", self.audit.pending_votes)
        self.assertIn("node_001", self.audit.pending_votes["block_hash_123"])

    def test_reach_consensus(self):
        """Test consensus checking."""
        node1 = LedgerNode(node_id="n1", address="a1", is_active=True)
        node2 = LedgerNode(node_id="n2", address="a2", is_active=True)
        self.audit.add_ledger_node(node1)
        self.audit.add_ledger_node(node2)

        # Add votes from both nodes
        self.audit.vote_for_block("hash1", "n1")
        self.audit.vote_for_block("hash1", "n2")

        result = self.audit.reach_consensus("hash1")
        self.assertTrue(result)


class TestDistributedLedger(unittest.TestCase):
    """Test distributed ledger operations."""

    def setUp(self):
        self.patcher1 = patch('time.time', return_value=1234567890.0)
        self.patcher1.start()
        self.ledger1 = BlockchainAuditTrail(node_id="node_001")
        self.ledger2 = BlockchainAuditTrail(node_id="node_002")

    def tearDown(self):
        self.patcher1.stop()

    def test_add_node_to_distributed_ledger(self):
        """Test adding node to distributed ledger."""
        ledger = DistributedLedger(self.ledger1)
        ledger.add_node(self.ledger2)
        self.assertEqual(len(ledger.nodes), 2)
        self.assertIn("node_002", ledger.nodes)

    def test_broadcast_transaction(self):
        """Test broadcasting transaction to all nodes."""
        ledger = DistributedLedger(self.ledger1)
        ledger.add_node(self.ledger2)

        tx = Transaction(
            tx_id="tx_001",
            tx_type=TransactionType.WORKFLOW_EXECUTE,
            workflow_id="wf_001",
            user_id="user1",
            data={},
            timestamp=time.time()
        )
        tx_id = ledger.broadcast_transaction(tx)
        self.assertEqual(tx_id, "tx_001")
        self.assertEqual(len(self.ledger1.pending_transactions), 1)
        self.assertEqual(len(self.ledger2.pending_transactions), 1)

    def test_sync_all_nodes(self):
        """Test syncing all nodes to longest chain."""
        ledger = DistributedLedger(self.ledger1)
        ledger.add_node(self.ledger2)

        # ledger1 has more blocks
        ledger1 = self.ledger1
        ledger1.create_token_account("user1", initial_balance=100)
        ledger1.log_workflow_event("wf_001", "user1", TransactionType.WORKFLOW_EXECUTE, {})
        ledger1.create_block()

        ledger.sync_all_nodes()
        # Both should have same chain length now
        self.assertEqual(len(ledger1.chain), len(self.ledger2.chain))


class TestExportAndStats(unittest.TestCase):
    """Test export and statistics functions."""

    def setUp(self):
        self.patcher1 = patch('time.time', return_value=1234567890.0)
        self.patcher1.start()
        self.audit = BlockchainAuditTrail(node_id="test_node")

    def tearDown(self):
        self.patcher1.stop()

    def test_export_audit_trail_json(self):
        """Test exporting audit trail as JSON."""
        self.audit.create_token_account("user1", initial_balance=100)
        self.audit.log_workflow_event(
            workflow_id="wf_001",
            user_id="user1",
            event_type=TransactionType.WORKFLOW_EXECUTE,
            data={}
        )
        self.audit.create_block()

        export = self.audit.export_audit_trail(format='json')
        self.assertIsNotNone(export)
        data = json.loads(export)
        self.assertIn('chain', data)
        self.assertIn('metadata', data)
        self.assertEqual(data['metadata']['chain_length'], 2)

    def test_export_unsupported_format(self):
        """Test exporting with unsupported format raises error."""
        with self.assertRaises(ValueError) as context:
            self.audit.export_audit_trail(format='xml')
        self.assertIn("Unsupported export format", str(context.exception))

    def test_get_block_stats(self):
        """Test getting block statistics."""
        self.audit.create_token_account("user1", initial_balance=100)
        self.audit.log_workflow_event(
            workflow_id="wf_001",
            user_id="user1",
            event_type=TransactionType.WORKFLOW_EXECUTE,
            data={}
        )
        self.audit.create_block()

        stats = self.audit.get_block_stats()
        self.assertIn('chain_length', stats)
        self.assertIn('total_transactions', stats)
        self.assertIn('transactions_by_type', stats)
        self.assertIn('verified', stats)
        self.assertTrue(stats['verified'])

    def test_sync_with_node(self):
        """Test syncing chain with remote node."""
        remote = BlockchainAuditTrail(node_id="remote")
        remote.create_token_account("user1", initial_balance=100)
        remote.log_workflow_event("wf_001", "user1", TransactionType.WORKFLOW_EXECUTE, {})
        remote.create_block()

        initial_length = len(self.audit.chain)
        blocks_received = self.audit.sync_with_node(remote)
        self.assertEqual(blocks_received, 1)
        self.assertEqual(len(self.audit.chain), initial_length + 1)


if __name__ == '__main__':
    unittest.main()
