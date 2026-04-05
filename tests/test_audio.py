from __future__ import annotations

from pathlib import Path

from bewley.cli import Project
from conftest import BewleyProject


class TestAudioIngestion:
    def test_add_audio_transcribes_and_links_audio(self, empty_project: BewleyProject, monkeypatch) -> None:
        audio_path = empty_project.root / "audio" / "alice.wav"
        audio_path.parent.mkdir(parents=True, exist_ok=True)
        audio_path.write_bytes(b"RIFF....WAVEfmt ")

        def fake_transcribe(
            self: Project,
            audio_path: Path,
            *,
            model: str,
            language: str | None,
            prompt: str | None,
            response_format: str,
        ) -> dict:
            assert audio_path.name == "alice.wav"
            assert model == "gpt-4o-transcribe-diarize"
            assert response_format == "diarized_json"
            return {
                "language": "en",
                "text": "Hello there. Thanks for joining.",
                "segments": [
                    {"start": 0.0, "end": 4.5, "speaker": "SPEAKER_00", "text": "Hello there."},
                    {"start": 4.5, "end": 8.0, "speaker": "SPEAKER_01", "text": "Thanks for joining."},
                ],
            }

        monkeypatch.setattr(Project, "transcribe_audio_with_openai", fake_transcribe)

        empty_project.cli_ok(
            "add-audio",
            "audio/alice.wav",
            "--output",
            "corpus/alice.txt",
            "--model",
            "gpt-4o-transcribe-diarize",
            "--response-format",
            "diarized_json",
        )

        transcript = (empty_project.root / "corpus" / "alice.txt").read_text(encoding="utf-8")
        assert "[00:00.00 - 00:04.50] SPEAKER_00: Hello there." in transcript
        assert "[00:04.50 - 00:08.00] SPEAKER_01: Thanks for joining." in transcript

        audio_stdout = empty_project.cli_ok("show", "audio", "corpus/alice.txt")
        assert "alice.wav" in audio_stdout
        assert "gpt-4o-transcribe-diarize" in audio_stdout
        assert "SPEAKER_00" in audio_stdout

        document_stdout = empty_project.cli_ok("show", "document", "corpus/alice.txt")
        assert "audio_source" in document_stdout
        assert "alice.wav" in document_stdout

    def test_show_audio_fails_for_plain_text_document(self, project: BewleyProject) -> None:
        code, _, stderr = project.cli("show", "audio", "corpus/interview_alice.txt")
        assert code != 0
        assert "no linked audio source" in stderr
