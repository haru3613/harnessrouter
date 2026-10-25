import json
import os
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import herdr_driver as hd  # noqa: E402


class FakeClient:
    def __init__(self, endpoint, token, timeout=15):
        self.endpoint, self.token, self.timeout = endpoint, token, timeout
        self.calls = []
        self.submit_failures = 0
        self.tasks = []

    def call(self, name, args):
        self.calls.append((name, dict(args)))
        if name == "submit_session_task":
            if self.submit_failures:
                self.submit_failures -= 1
                raise hd.TransportError("lost reply")
            return {"task": {"id": "task-1", "execution_state": "queued"}}
        if name == "get_task":
            return {"task": self.tasks.pop(0) if self.tasks else {"id": "task-1", "execution_state": "succeeded", "result": "done"}}
        if name == "cancel_session_task":
            return {"task": {"id": "task-1"}}
        raise AssertionError(name)


class HerdrDriverTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.cwd = pathlib.Path(self.tmp.name)
        self.events = []
        self.clients = []
        def factory(*args, **kwargs):
            client = FakeClient(*args, **kwargs); self.clients.append(client); return client
        self.factory = factory
        self.env = {"HARU_WORKSPACE": "fixture", "HR_HERDR_TOKEN": "private-token"}

    def tearDown(self):
        self.tmp.cleanup()

    def job(self, **changes):
        result = {"cwd": str(self.cwd), "base_url": "https://dispatch.example/mcp", "workspace": "fixture",
                  "request_id": "request-1", "prompt": "Reply with one fact.", "model": "haru-codex",
                  "timeout_seconds": 3}
        result.update(changes)
        return result

    def turn(self, job=None, now=None):
        return hd.Turn(job or self.job(), environ=self.env, client_factory=self.factory,
                       emit=lambda m, p: self.events.append((m, p)), now=now or (lambda: 0), sleep=lambda _: None)

    def test_submits_only_read_only_evidence_required_task_and_hides_token(self):
        turn = self.turn()
        self.assertEqual(turn.run(), 0)
        call = self.clients[0].calls[0]
        self.assertEqual(call[0], "submit_session_task")
        self.assertEqual(call[1]["mode"], "analysis")
        self.assertEqual(call[1]["result_contract"], "evidence_required")
        self.assertEqual(call[1]["requested_provider"], "codex")
        self.assertNotIn("private-token", json.dumps(self.events))
        self.assertEqual(self.events[-1][1]["status"], "completed")

    def test_lost_submit_reply_retries_exact_idempotent_payload_once(self):
        turn = self.turn(); turn.client.submit_failures = 1
        turn.run()
        calls = [args for name, args in turn.client.calls if name == "submit_session_task"]
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0], calls[1])
        state = json.loads((self.cwd / hd.STATE_DIR / "request-1.json").read_text())
        self.assertEqual(state["task_id"], "task-1")
        self.assertEqual((self.cwd / hd.STATE_DIR / "request-1.json").stat().st_mode & 0o777, 0o600)

    def test_restart_uses_saved_task_and_auth_change_fails_closed(self):
        first = self.turn(); first.run()
        second = self.turn(); second.run()
        self.assertFalse(any(n == "submit_session_task" for n, _ in second.client.calls))
        changed = dict(self.env, HR_HERDR_TOKEN="different")
        with self.assertRaisesRegex(hd.DriverError, "does not match"):
            hd.Turn(self.job(), environ=changed, client_factory=self.factory)

    def test_waiting_input_and_uncertain_are_honest_incomplete(self):
        turn = self.turn(); turn.client.tasks = [{"id": "task-1", "execution_state": "waiting_input"}]
        turn.run(); self.assertEqual(self.events[-1][1]["status"], "incomplete")
        self.events.clear(); turn = self.turn(); turn.client.tasks = [{"id": "task-1", "execution_state": "uncertain"}]
        turn.run(); self.assertEqual(self.events[-1][1]["reason"], "HARU task outcome is not confirmed")

    def test_cancel_is_always_stop_unknown(self):
        turn = self.turn(); turn.state["task_id"] = "task-1"; turn._save()
        self.assertEqual(turn.cancel(), "remote_stop_unknown")
        self.assertEqual([name for name, _ in turn.client.calls[-2:]], ["get_task", "cancel_session_task"])
        turn.close()

    def test_rejects_paths_unsupported_inputs_and_bad_endpoint(self):
        for changes in ({"workspace": "/tmp/x"}, {"files": [{"x": 1}]}, {"base_url": "http://dispatch.example/mcp"},
                        {"model": "gpt-paid"}, {"resume_session_id": "codex:one"}):
            with self.subTest(changes=changes):
                with self.assertRaises(hd.DriverError): self.turn(self.job(**changes))

    def test_job_schema_timeout_and_state_path_hardening(self):
        long_turn = self.turn(self.job(timeout_seconds=21600)); long_turn.close()
        with self.assertRaises(hd.DriverError): self.turn(self.job(timeout_seconds=21601))
        with self.assertRaises(hd.DriverError): self.turn(self.job(untrusted_capability="x"))
        hostile_cwd = self.cwd / "hostile"; hostile_cwd.mkdir()
        state_root = hostile_cwd / ".harness"
        outside = hostile_cwd / "outside"; outside.mkdir()
        state_root.symlink_to(outside, target_is_directory=True)
        with self.assertRaisesRegex(hd.DriverError, "symlink"):
            self.turn(self.job(cwd=str(hostile_cwd), request_id="hostile"))
        first = self.turn()
        with self.assertRaisesRegex(hd.DriverError, "already active"):
            self.turn()
        first.close()
        self.turn().close()

    def test_mcp_content_parser_refuses_error_and_accepts_structured(self):
        self.assertEqual(hd._content_result({"result": {"structuredContent": {"task": {}}}}), {"task": {}})
        with self.assertRaises(hd.DriverError): hd._content_result({"error": {"message": "secret"}})

    def test_final_output_redacts_driver_token(self):
        turn = self.turn()
        turn.client.tasks = [{"id": "task-1", "execution_state": "succeeded", "result": "private-token must not leave driver"}]
        turn.run()
        rendered = json.dumps(self.events)
        self.assertNotIn("private-token", rendered)
        self.assertIn("[redacted]", rendered)

    def test_all_event_fields_redact_token_before_truncation(self):
        turn = self.turn()
        turn.emit("notice", {"text": "private-token", "nested": {"fact": "private-token"}})
        crossing = "x" * (hd.MAX_RESULT - 2) + "private-token"
        self.assertNotIn("private-token", hd._safe_output(crossing, "private-token"))
        turn.client.tasks = [{"id": "task-1", "execution_state": "succeeded", "result": "ok",
                              "selected_provider": "private-token"}]
        turn.run()
        self.assertNotIn("private-token", json.dumps(self.events))

    def test_failed_reason_redacts_token_and_transport_is_unknown_not_failed(self):
        turn = self.turn()
        turn.client.tasks = [{"id": "task-1", "execution_state": "failed", "result": "private-token leaked"}]
        turn.run()
        self.assertNotIn("private-token", json.dumps(self.events))
        self.assertEqual(self.events[-1][1]["status"], "failed")

        class PollBreaks(FakeClient):
            def call(self, name, args):
                if name == "get_task":
                    raise hd.TransportError("lost poll")
                return super().call(name, args)
        clients = []
        def factory(*args, **kwargs):
            c = PollBreaks(*args, **kwargs); clients.append(c); return c
        events = []
        uncertain = hd.Turn(self.job(request_id="uncertain"), environ=self.env, client_factory=factory,
                            emit=lambda m, p: events.append((m, p)), now=lambda: 0, sleep=lambda _: None)
        uncertain.run()
        self.assertEqual(events[-1][1]["status"], "incomplete")
        self.assertEqual(events[-1][1]["reason"], "remote_outcome_unknown")
        state = json.loads((self.cwd / hd.STATE_DIR / "uncertain.json").read_text())
        self.assertTrue(state["submission_attempted"])

    def test_malformed_postsubmit_response_is_unknown_not_definitive_failure(self):
        class MalformedSubmit(FakeClient):
            def call(self, name, args):
                if name == "submit_session_task":
                    return {"task": {"id": "not allowed spaces"}}
                return super().call(name, args)
        events = []
        turn = hd.Turn(self.job(request_id="malformed"), environ=self.env, client_factory=MalformedSubmit,
                       emit=lambda m, p: events.append((m, p)), now=lambda: 0, sleep=lambda _: None)
        turn.run()
        self.assertEqual(events[-1][1]["status"], "incomplete")
        self.assertEqual(events[-1][1]["reason"], "remote_outcome_unknown")

    def test_cancel_before_submit_is_unknown_after_durable_intent(self):
        turn = self.turn()
        turn.state["submission_attempted"] = True
        turn._save()
        self.assertEqual(turn.cancel(), "remote_stop_unknown")
        turn.close()

    def test_continuation_requires_owned_live_predecessor(self):
        first = self.turn()
        first.run()
        second = self.turn(self.job(request_id="request-2", resume_session_id="haru:task-1"))
        second.client.tasks = [{"id": "task-1", "execution_state": "succeeded", "return_state": "handled",
                                "resource": {"state": "cleanup_pending"}}]
        second.run()
        submit = next(args for name, args in second.client.calls if name == "submit_session_task")
        self.assertEqual(submit["continue_task_id"], "task-1")

        self.events.clear()
        refused = self.turn(self.job(request_id="request-3", resume_session_id="haru:task-1"))
        refused.client.tasks = [{"id": "task-1", "execution_state": "succeeded", "return_state": "handled",
                                 "resource": {"state": "closed"}}]
        refused.run()
        self.assertEqual(self.events[-1][1]["status"], "failed")
        self.assertFalse(any(name == "submit_session_task" for name, _ in refused.client.calls))


if __name__ == "__main__":
    unittest.main()
