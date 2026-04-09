"""Comprehensive tests for action modules.

Tests each action type with various parameter combinations.
Mock external dependencies (pyautogui, cv2) where needed.
"""

import sys
import os
import time
import unittest
from unittest.mock import patch, MagicMock, Mock

# Add project root to path (parent of rabai_autoclick package directory)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from rabai_autoclick.core.context import ContextManager
from rabai_autoclick.core.action_loader import ActionLoader


def make_loader():
    loader = ActionLoader()
    loader.load_all()
    return loader


class TestClickAction(unittest.TestCase):
    """Tests for ClickAction."""

    @patch('pyautogui.click')
    def test_click_basic(self, mock_click):
        loader = make_loader()
        action = loader.get_action("click")()
        ctx = ContextManager()
        result = action.execute(ctx, {"x": 100, "y": 200})
        self.assertTrue(result.success)
        mock_click.assert_called_once()

    @patch('pyautogui.click')
    def test_click_with_button(self, mock_click):
        loader = make_loader()
        action = loader.get_action("click")()
        ctx = ContextManager()
        result = action.execute(ctx, {"x": 100, "y": 200, "button": "right"})
        self.assertTrue(result.success)

    def test_click_invalid_button(self):
        loader = make_loader()
        action = loader.get_action("click")()
        ctx = ContextManager()
        result = action.execute(ctx, {"x": 100, "y": 200, "button": "invalid"})
        self.assertFalse(result.success)
        self.assertIn("must be one of", result.message)

    def test_click_missing_x(self):
        loader = make_loader()
        action = loader.get_action("click")()
        ctx = ContextManager()
        result = action.execute(ctx, {"y": 200})
        # x defaults to 0, so it's valid
        self.assertTrue(result.success)


class TestDelayAction(unittest.TestCase):
    """Tests for DelayAction."""

    def test_delay_seconds(self):
        loader = make_loader()
        action = loader.get_action("delay")()
        ctx = ContextManager()
        start = time.time()
        result = action.execute(ctx, {"seconds": 0.05})
        elapsed = time.time() - start
        self.assertTrue(result.success)
        self.assertGreaterEqual(elapsed, 0.04)
        self.assertLess(elapsed, 0.3)

    def test_delay_human_format(self):
        loader = make_loader()
        action = loader.get_action("delay")()
        ctx = ContextManager()
        start = time.time()
        result = action.execute(ctx, {"seconds": "0.05"})
        elapsed = time.time() - start
        self.assertTrue(result.success)

    def test_delay_negative(self):
        loader = make_loader()
        action = loader.get_action("delay")()
        ctx = ContextManager()
        result = action.execute(ctx, {"seconds": -1})
        self.assertFalse(result.success)


class TestConditionAction(unittest.TestCase):
    """Tests for ConditionAction."""

    def test_true_condition(self):
        loader = make_loader()
        action = loader.get_action("condition")()
        ctx = ContextManager()
        ctx.set("x", 10)
        ctx.set("y", 20)
        result = action.execute(ctx, {"condition": "x < y", "true_next": "step2"})
        self.assertTrue(result.success)
        self.assertEqual(result.next_step_id, "step2")
        self.assertEqual(result.data["result"], True)

    def test_false_condition(self):
        loader = make_loader()
        action = loader.get_action("condition")()
        ctx = ContextManager()
        ctx.set("x", 30)
        ctx.set("y", 20)
        result = action.execute(ctx, {"condition": "x < y", "true_next": "step2", "false_next": "step3"})
        self.assertTrue(result.success)
        self.assertEqual(result.next_step_id, "step3")

    def test_empty_condition(self):
        loader = make_loader()
        action = loader.get_action("condition")()
        ctx = ContextManager()
        result = action.execute(ctx, {})
        self.assertFalse(result.success)
        self.assertIn("条件表达式为空", result.message)

    def test_condition_equals(self):
        loader = make_loader()
        action = loader.get_action("condition")()
        ctx = ContextManager()
        ctx.set("name", "test")
        result = action.execute(ctx, {"condition": "name == 'test'", "true_next": "step2"})
        self.assertTrue(result.success)
        self.assertEqual(result.next_step_id, "step2")

    def test_condition_in(self):
        loader = make_loader()
        action = loader.get_action("condition")()
        ctx = ContextManager()
        ctx.set("items", [1, 2, 3])
        result = action.execute(ctx, {"condition": "2 in items", "true_next": "step2"})
        self.assertTrue(result.success)
        self.assertEqual(result.next_step_id, "step2")


class TestLoopAction(unittest.TestCase):
    """Tests for LoopAction."""

    def test_loop_iterations(self):
        loader = make_loader()
        action = loader.get_action("loop")()
        ctx = ContextManager()

        # First call (iteration 0)
        r1 = action.execute(ctx, {"loop_id": "loop1", "count": 3, "loop_start": "step1", "loop_end": "step2"})
        self.assertTrue(r1.success)
        self.assertEqual(r1.next_step_id, "step1")

        # Second call (iteration 1)
        r2 = action.execute(ctx, {"loop_id": "loop1", "count": 3, "loop_start": "step1", "loop_end": "step2"})
        self.assertTrue(r2.success)
        self.assertEqual(r2.next_step_id, "step1")

        # Third call (iteration 2)
        r3 = action.execute(ctx, {"loop_id": "loop1", "count": 3, "loop_start": "step1", "loop_end": "step2"})
        self.assertTrue(r3.success)
        self.assertEqual(r3.next_step_id, "step1")

        # Fourth call (iteration 3 >= count, ends)
        r4 = action.execute(ctx, {"loop_id": "loop1", "count": 3, "loop_start": "step1", "loop_end": "step2"})
        self.assertTrue(r4.success)
        self.assertEqual(r4.next_step_id, "step2")

    def test_loop_zero_count(self):
        loader = make_loader()
        action = loader.get_action("loop")()
        ctx = ContextManager()
        result = action.execute(ctx, {"loop_id": "l1", "count": 0})
        self.assertFalse(result.success)


class TestSetVariableAction(unittest.TestCase):
    """Tests for SetVariableAction."""

    def test_set_int(self):
        loader = make_loader()
        action = loader.get_action("set_variable")()
        ctx = ContextManager()
        result = action.execute(ctx, {"name": "count", "value": 42, "value_type": "int"})
        self.assertTrue(result.success)
        self.assertEqual(ctx.get("count"), 42)

    def test_set_string(self):
        loader = make_loader()
        action = loader.get_action("set_variable")()
        ctx = ContextManager()
        result = action.execute(ctx, {"name": "name", "value": "test"})
        self.assertTrue(result.success)
        self.assertEqual(ctx.get("name"), "test")

    def test_set_float(self):
        loader = make_loader()
        action = loader.get_action("set_variable")()
        ctx = ContextManager()
        result = action.execute(ctx, {"name": "pi", "value": 3.14, "value_type": "float"})
        self.assertTrue(result.success)
        self.assertAlmostEqual(ctx.get("pi"), 3.14)

    def test_set_bool(self):
        loader = make_loader()
        action = loader.get_action("set_variable")()
        ctx = ContextManager()
        result = action.execute(ctx, {"name": "flag", "value": True, "value_type": "bool"})
        self.assertTrue(result.success)
        self.assertEqual(ctx.get("flag"), True)

    def test_set_list(self):
        loader = make_loader()
        action = loader.get_action("set_variable")()
        ctx = ContextManager()
        result = action.execute(ctx, {"name": "items", "value": [1, 2, 3], "value_type": "list"})
        self.assertTrue(result.success)
        self.assertEqual(ctx.get("items"), [1, 2, 3])

    def test_set_dict(self):
        loader = make_loader()
        action = loader.get_action("set_variable")()
        ctx = ContextManager()
        # SetVariableAction needs JSON string for dict type
        result = action.execute(ctx, {"name": "data", "value": '{"a": 1}', "value_type": "dict"})
        self.assertTrue(result.success)
        self.assertEqual(ctx.get("data"), {"a": 1})

    def test_set_tuple(self):
        loader = make_loader()
        action = loader.get_action("set_variable")()
        ctx = ContextManager()
        # SetVariableAction needs JSON string for tuple type
        result = action.execute(ctx, {"name": "coords", "value": "[10, 20]", "value_type": "tuple"})
        self.assertTrue(result.success)
        self.assertEqual(ctx.get("coords"), (10, 20))

    def test_set_none(self):
        loader = make_loader()
        action = loader.get_action("set_variable")()
        ctx = ContextManager()
        # For 'none' type, value can be string 'null' or None
        result = action.execute(ctx, {"name": "empty", "value": "null", "value_type": "none"})
        self.assertTrue(result.success)
        self.assertIsNone(ctx.get("empty"))


class TestTypeAction(unittest.TestCase):
    """Tests for TypeAction (keyboard input)."""

    @patch('pyautogui.write')
    def test_type_basic(self, mock_write):
        loader = make_loader()
        action = loader.get_action("type_text")()
        ctx = ContextManager()
        result = action.execute(ctx, {"text": "hello"})
        self.assertTrue(result.success)


class TestKeyPressAction(unittest.TestCase):
    """Tests for KeyPressAction."""

    @patch('pyautogui.press')
    def test_key_press_single(self, mock_press):
        loader = make_loader()
        action = loader.get_action("key_press")()
        ctx = ContextManager()
        result = action.execute(ctx, {"key": "enter"})
        self.assertTrue(result.success)
        mock_press.assert_called_with('enter')

    @patch('pyautogui.keyDown')
    @patch('pyautogui.keyUp')
    def test_key_press_combo(self, mock_keyUp, mock_keyDown):
        loader = make_loader()
        action = loader.get_action("key_press")()
        ctx = ContextManager()
        result = action.execute(ctx, {"keys": ["ctrl", "c"]})
        self.assertTrue(result.success)
        mock_keyDown.assert_called()
        mock_keyUp.assert_called()


class TestCommentAction(unittest.TestCase):
    """Tests for CommentAction."""

    def test_comment_basic(self):
        loader = make_loader()
        action = loader.get_action("comment")()
        ctx = ContextManager()
        result = action.execute(ctx, {"text": "This is a comment"})
        self.assertTrue(result.success)
        self.assertIn("This is a comment", result.message)

    def test_comment_with_output_var(self):
        loader = make_loader()
        action = loader.get_action("comment")()
        ctx = ContextManager()
        result = action.execute(ctx, {"text": "Note", "output_var": "my_comment"})
        self.assertTrue(result.success)
        self.assertEqual(ctx.get("my_comment"), "Note")


class TestLogAction(unittest.TestCase):
    """Tests for LogAction."""

    def test_log_info(self):
        loader = make_loader()
        action = loader.get_action("log")()
        ctx = ContextManager()
        result = action.execute(ctx, {"message": "Test log", "level": "info"})
        self.assertTrue(result.success)
        self.assertIn("Test log", result.message)

    def test_log_with_output_var(self):
        loader = make_loader()
        action = loader.get_action("log")()
        ctx = ContextManager()
        result = action.execute(ctx, {"message": "Log message", "output_var": "logged"})
        self.assertTrue(result.success)
        self.assertEqual(ctx.get("logged"), "Log message")


class TestAssertAction(unittest.TestCase):
    """Tests for AssertAction."""

    def test_assert_true(self):
        loader = make_loader()
        action = loader.get_action("assert")()
        ctx = ContextManager()
        ctx.set("x", 10)
        result = action.execute(ctx, {"condition": "{{x > 5}}"})
        self.assertTrue(result.success)
        self.assertIn("断言通过", result.message)

    def test_assert_false(self):
        loader = make_loader()
        action = loader.get_action("assert")()
        ctx = ContextManager()
        ctx.set("x", 3)
        # AssertAction uses condition without {{}} wrapper
        result = action.execute(ctx, {"condition": "x > 5"})
        self.assertFalse(result.success)
        self.assertIn("断言失败", result.message)


class TestTryCatchAction(unittest.TestCase):
    """Tests for TryCatchAction."""

    def test_try_catch_no_exception(self):
        loader = make_loader()
        action = loader.get_action("try_catch")()
        ctx = ContextManager()
        result = action.execute(ctx, {"try_steps": ["step1"], "catch_steps": ["step2"]})
        self.assertTrue(result.success)
        self.assertEqual(result.next_step_id, "step1")
        self.assertEqual(result.data["branch"], "try")

    def test_try_catch_with_exception(self):
        loader = make_loader()
        action = loader.get_action("try_catch")()
        ctx = ContextManager()
        ctx.set("_exception", "Some error")
        result = action.execute(ctx, {"try_steps": ["step1"], "catch_steps": ["step2"]})
        self.assertTrue(result.success)
        self.assertEqual(result.next_step_id, "step2")
        self.assertEqual(result.data["branch"], "catch")


class TestThrowAction(unittest.TestCase):
    """Tests for ThrowAction."""

    def test_throw_basic(self):
        loader = make_loader()
        action = loader.get_action("throw")()
        ctx = ContextManager()
        result = action.execute(ctx, {"message": "Test error"})
        self.assertFalse(result.success)
        self.assertEqual(ctx.get("_exception"), "RuntimeError: Test error")


class TestForEachAction(unittest.TestCase):
    """Tests for ForEachAction."""

    def test_for_each_list(self):
        loader = make_loader()
        action = loader.get_action("for_each")()
        ctx = ContextManager()
        ctx.set("items", [1, 2, 3])

        # First iteration
        r1 = action.execute(ctx, {"items": "{{items}}", "loop_start": "step1", "loop_end": "step2"})
        self.assertTrue(r1.success)
        self.assertEqual(r1.next_step_id, "step1")
        self.assertEqual(ctx.get("_for_item"), 1)
        self.assertEqual(ctx.get("_for_index"), 0)

        # Second iteration
        r2 = action.execute(ctx, {"items": "{{items}}", "loop_start": "step1", "loop_end": "step2"})
        self.assertTrue(r2.success)
        self.assertEqual(ctx.get("_for_item"), 2)
        self.assertEqual(ctx.get("_for_index"), 1)

        # Third iteration
        r3 = action.execute(ctx, {"items": "{{items}}", "loop_start": "step1", "loop_end": "step2"})
        self.assertTrue(r3.success)
        self.assertEqual(ctx.get("_for_item"), 3)
        self.assertEqual(ctx.get("_for_index"), 2)

        # Fourth - ends
        r4 = action.execute(ctx, {"items": "{{items}}", "loop_start": "step1", "loop_end": "step2"})
        self.assertTrue(r4.success)
        self.assertEqual(r4.next_step_id, "step2")


class TestGotoAction(unittest.TestCase):
    """Tests for GotoAction."""

    def test_goto_basic(self):
        loader = make_loader()
        action = loader.get_action("goto")()
        ctx = ContextManager()
        result = action.execute(ctx, {"label": "cleanup"})
        self.assertTrue(result.success)
        self.assertEqual(result.next_step_id, "cleanup")

    def test_goto_conditional_true(self):
        loader = make_loader()
        action = loader.get_action("goto")()
        ctx = ContextManager()
        ctx.set("x", 10)
        result = action.execute(ctx, {"label": "step2", "condition": "{{x > 5}}"})
        self.assertTrue(result.success)
        self.assertEqual(result.next_step_id, "step2")


class TestActionLoader(unittest.TestCase):
    """Tests for ActionLoader."""

    def test_load_all_actions(self):
        loader = make_loader()
        actions = loader.load_all()
        self.assertGreater(len(actions), 0)
        self.assertIn("click", actions)
        self.assertIn("delay", actions)
        self.assertIn("condition", actions)

    def test_get_action(self):
        loader = make_loader()
        click_action = loader.get_action("click")
        self.assertIsNotNone(click_action)
        self.assertTrue(hasattr(click_action, 'execute'))


class TestContextManagerActions(unittest.TestCase):
    """Tests for ContextManager interaction with actions."""

    def test_bracket_notation(self):
        ctx = ContextManager()
        ctx.set("obj", {"key": "value"})
        result = ctx.resolve_value("{{obj['key']}}")
        self.assertEqual(result, "value")

    def test_expression_caching(self):
        ctx = ContextManager()
        ctx.set("a", 5)
        r1 = ctx.resolve_value("{{a + 3}}")
        r2 = ctx.resolve_value("{{a + 3}}")
        self.assertEqual(r1, r2)


class TestWaitForActions(unittest.TestCase):
    """Tests for wait_for actions (mocked)."""

    def test_wait_for_image_action_class(self):
        loader = make_loader()
        self.assertIn("wait_for_image", loader._actions)
        action = loader.get_action("wait_for_image")()
        self.assertTrue(hasattr(action, 'execute'))

    def test_wait_for_text_action_class(self):
        loader = make_loader()
        self.assertIn("wait_for_text", loader._actions)
        action = loader.get_action("wait_for_text")()
        self.assertTrue(hasattr(action, 'execute'))

    def test_wait_for_element_action_class(self):
        loader = make_loader()
        self.assertIn("wait_for_element", loader._actions)
        action = loader.get_action("wait_for_element")()
        self.assertTrue(hasattr(action, 'execute'))


class TestLoopWhileActions(unittest.TestCase):
    """Tests for loop_while actions."""

    def test_loop_while_class(self):
        loader = make_loader()
        self.assertIn("loop_while", loader._actions)
        action = loader.get_action("loop_while")()
        self.assertTrue(hasattr(action, 'execute'))

    def test_loop_while_break_class(self):
        loader = make_loader()
        self.assertIn("loop_while_break", loader._actions)

    def test_loop_while_continue_class(self):
        loader = make_loader()
        self.assertIn("loop_while_continue", loader._actions)


class TestNotifyActions(unittest.TestCase):
    """Tests for notify actions."""

    def test_notify_class(self):
        loader = make_loader()
        self.assertIn("notify", loader._actions)
        action = loader.get_action("notify")()
        self.assertTrue(hasattr(action, 'execute'))

    def test_email_notify_class(self):
        loader = make_loader()
        self.assertIn("email_notify", loader._actions)
        action = loader.get_action("email_notify")()
        self.assertTrue(hasattr(action, 'execute'))

    def test_webhook_notify_class(self):
        loader = make_loader()
        self.assertIn("webhook_notify", loader._actions)
        action = loader.get_action("webhook_notify")()
        self.assertTrue(hasattr(action, 'execute'))

    def test_slack_notify_class(self):
        loader = make_loader()
        self.assertIn("slack_notify", loader._actions)
        action = loader.get_action("slack_notify")()
        self.assertTrue(hasattr(action, 'execute'))


class TestFilesystemActions(unittest.TestCase):
    """Tests for filesystem actions."""

    def test_file_read_class(self):
        loader = make_loader()
        self.assertIn("file_read", loader._actions)
        action = loader.get_action("file_read")()
        self.assertTrue(hasattr(action, 'execute'))

    def test_file_write_class(self):
        loader = make_loader()
        self.assertIn("file_write", loader._actions)
        action = loader.get_action("file_write")()
        self.assertTrue(hasattr(action, 'execute'))

    def test_file_exists_class(self):
        loader = make_loader()
        self.assertIn("file_exists", loader._actions)
        action = loader.get_action("file_exists")()
        self.assertTrue(hasattr(action, 'execute'))

    def test_file_delete_class(self):
        loader = make_loader()
        self.assertIn("file_delete", loader._actions)
        action = loader.get_action("file_delete")()
        self.assertTrue(hasattr(action, 'execute'))

    def test_file_copy_class(self):
        loader = make_loader()
        self.assertIn("file_copy", loader._actions)
        action = loader.get_action("file_copy")()
        self.assertTrue(hasattr(action, 'execute'))

    def test_dir_create_class(self):
        loader = make_loader()
        self.assertIn("dir_create", loader._actions)
        action = loader.get_action("dir_create")()
        self.assertTrue(hasattr(action, 'execute'))

    def test_file_list_class(self):
        loader = make_loader()
        self.assertIn("file_list", loader._actions)
        action = loader.get_action("file_list")()
        self.assertTrue(hasattr(action, 'execute'))

    def test_file_move_class(self):
        loader = make_loader()
        self.assertIn("file_move", loader._actions)
        action = loader.get_action("file_move")()
        self.assertTrue(hasattr(action, 'execute'))


class TestNetworkActions(unittest.TestCase):
    """Tests for network actions."""

    def test_http_get_class(self):
        loader = make_loader()
        self.assertIn("http_get", loader._actions)
        action = loader.get_action("http_get")()
        self.assertTrue(hasattr(action, 'execute'))

    def test_http_post_class(self):
        loader = make_loader()
        self.assertIn("http_post", loader._actions)
        action = loader.get_action("http_post")()
        self.assertTrue(hasattr(action, 'execute'))

    def test_download_file_class(self):
        loader = make_loader()
        self.assertIn("download_file", loader._actions)
        action = loader.get_action("download_file")()
        self.assertTrue(hasattr(action, 'execute'))


class TestNewActionLoader(unittest.TestCase):
    """Tests for ActionLoader with new actions."""

    def test_load_all_includes_new_actions(self):
        loader = make_loader()
        actions = loader.load_all()
        
        # Notify actions
        self.assertIn("notify", actions)
        self.assertIn("email_notify", actions)
        self.assertIn("webhook_notify", actions)
        self.assertIn("slack_notify", actions)
        
        # Filesystem actions
        self.assertIn("file_read", actions)
        self.assertIn("file_write", actions)
        self.assertIn("file_exists", actions)
        self.assertIn("file_delete", actions)
        self.assertIn("file_copy", actions)
        self.assertIn("dir_create", actions)
        self.assertIn("file_list", actions)
        self.assertIn("file_move", actions)
        
        # Network actions
        self.assertIn("http_get", actions)
        self.assertIn("http_post", actions)
        self.assertIn("download_file", actions)


if __name__ == '__main__':
    unittest.main()
