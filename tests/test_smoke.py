from __future__ import annotations

import contextlib
import io
import os
import tempfile
import unittest
from pathlib import Path

from bewley.cli import main


class BewleySmokeTest(unittest.TestCase):
    def run_cli(self, cwd: Path, *args: str, human: bool = True) -> tuple[int, str, str]:
        old_cwd = Path.cwd()
        stdout = io.StringIO()
        stderr = io.StringIO()
        cli_args = list(args)
        if human:
            cli_args = ["-H"] + cli_args
        try:
            os.chdir(cwd)
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                code = main(cli_args)
        finally:
            os.chdir(old_cwd)
        return code, stdout.getvalue(), stderr.getvalue()

    def test_init_add_annotate_update_query_and_fsck(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)

            code, _, _ = self.run_cli(root, "init")
            self.assertEqual(code, 0)

            corpus = root / "corpus"
            (corpus / "interview.txt").write_text("alpha\nbeta\ngamma\n", encoding="utf-8")

            code, _, _ = self.run_cli(root, "add", "corpus/interview.txt")
            self.assertEqual(code, 0)

            code, _, _ = self.run_cli(root, "code", "create", "trust")
            self.assertEqual(code, 0)

            code, stdout, _ = self.run_cli(root, "annotate", "apply", "trust", "corpus/interview.txt", "--lines", "2:2")
            self.assertEqual(code, 0)
            annotation_id = stdout.strip()
            self.assertTrue(annotation_id)

            (corpus / "interview.txt").write_text("intro\nalpha\nbeta\ngamma\n", encoding="utf-8")
            code, _, _ = self.run_cli(root, "update", "corpus/interview.txt")
            self.assertEqual(code, 0)

            code, stdout, _ = self.run_cli(root, "show", "snippets", "--code", "trust")
            self.assertEqual(code, 0)
            self.assertIn("relocated", stdout)
            self.assertIn("beta", stdout)

            code, stdout, _ = self.run_cli(root, "export", "snippets", "--code", "trust", "--format", "jsonl", "--context-lines", "1")
            self.assertEqual(code, 0)
            self.assertIn('"selected_text": "beta\\n"', stdout)
            self.assertIn('"context_before": "alpha"', stdout)
            self.assertIn('"context_after": "gamma"', stdout)

            code, stdout, _ = self.run_cli(root, "export", "quotes", "--code", "trust", "--format", "jsonl", "--context-lines", "1")
            self.assertEqual(code, 0)
            self.assertIn('"exact_text": "beta\\n"', stdout)
            self.assertIn('"start_byte": 12', stdout)
            self.assertIn('"end_byte": 17', stdout)
            self.assertIn('"start_line": 3', stdout)
            self.assertIn('"end_line": 3', stdout)
            self.assertIn('"context_before": "alpha"', stdout)
            self.assertIn('"context_after": "gamma"', stdout)

            code, stdout, _ = self.run_cli(root, "query", "trust", "--mode", "document")
            self.assertEqual(code, 0)
            self.assertIn("corpus/interview.txt", stdout)

            code, stdout, _ = self.run_cli(root, "export", "html", "--output", "report/codes.html", "--title", "Interview Explorer")
            self.assertEqual(code, 0)
            report_path = Path(stdout.strip())
            self.assertTrue(report_path.exists())
            html = report_path.read_text(encoding="utf-8")
            self.assertIn("Interview Explorer", html)
            self.assertIn("trust", html)
            self.assertIn("beta", html)

            code, stdout, _ = self.run_cli(
                root,
                "export",
                "document-html",
                "corpus/interview.txt",
                "--output",
                "report/document.html",
                "--title",
                "Annotated Interview",
            )
            self.assertEqual(code, 0)
            document_report_path = Path(stdout.strip())
            self.assertTrue(document_report_path.exists())
            document_html = document_report_path.read_text(encoding="utf-8")
            self.assertIn("Annotated Interview", document_html)
            self.assertIn("corpus/interview.txt", document_html)
            self.assertIn("trust", document_html)
            self.assertIn("beta", document_html)
            self.assertIn("anno-segment", document_html)

            code, stdout, _ = self.run_cli(root, "fsck")
            self.assertEqual(code, 0)
            self.assertIn("ok", stdout)


if __name__ == "__main__":
    unittest.main()
