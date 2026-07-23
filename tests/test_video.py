from __future__ import annotations

from pathlib import Path

from bewley.cli import Project
from conftest import BewleyProject


class TestVideoIngestion:
    def test_add_video_chunks_audio_and_links_video(self, empty_project: BewleyProject, monkeypatch) -> None:
        video_path = empty_project.root / "video" / "alice.mp4"
        video_path.parent.mkdir(parents=True, exist_ok=True)
        video_path.write_bytes(b"\x00\x00\x00\x18ftypmp42")

        def fake_chunk_plan(
            self: Project,
            video_path: Path,
            *,
            max_upload_bytes: int,
            overlap_seconds: float,
            audio_bitrate_kbps: int,
        ) -> list[dict]:
            assert video_path.name == "alice.mp4"
            assert overlap_seconds == 2.0
            assert audio_bitrate_kbps == 64
            return [
                {
                    "chunk_index": 0,
                    "extract_start_seconds": 0.0,
                    "extract_end_seconds": 10.0,
                    "logical_start_seconds": 0.0,
                    "logical_end_seconds": 10.0,
                },
                {
                    "chunk_index": 1,
                    "extract_start_seconds": 8.0,
                    "extract_end_seconds": 18.0,
                    "logical_start_seconds": 10.0,
                    "logical_end_seconds": 18.0,
                },
            ]

        def fake_extract(
            self: Project,
            video_path: Path,
            chunk_path: Path,
            *,
            extract_start_seconds: float,
            extract_end_seconds: float,
            audio_bitrate_kbps: int,
        ) -> None:
            chunk_path.write_bytes(f"{extract_start_seconds}-{extract_end_seconds}".encode("utf-8"))

        def fake_transcribe(
            self: Project,
            audio_path: Path,
            *,
            model: str,
            language: str | None,
            prompt: str | None,
            response_format: str,
        ) -> dict:
            assert model == "gpt-4o-transcribe-diarize"
            assert response_format == "diarized_json"
            if "chunk-000" in audio_path.name:
                return {
                    "language": "en",
                    "segments": [
                        {"start": 0.0, "end": 4.0, "speaker": "SPEAKER_00", "text": "Opening remarks."},
                    ],
                }
            return {
                "language": "en",
                "segments": [
                    {"start": 3.0, "end": 6.0, "speaker": "SPEAKER_01", "text": "Later response."},
                ],
            }

        monkeypatch.setattr(Project, "build_video_chunk_plan", fake_chunk_plan)
        monkeypatch.setattr(Project, "extract_audio_chunk", fake_extract)
        monkeypatch.setattr(Project, "transcribe_audio_with_openai", fake_transcribe)

        empty_project.cli_ok(
            "add-video",
            "video/alice.mp4",
            "--output",
            "corpus/alice-video.txt",
            "--model",
            "gpt-4o-transcribe-diarize",
            "--response-format",
            "diarized_json",
            "--audio-bitrate-kbps",
            "64",
            "--chunk-overlap-seconds",
            "2",
        )

        transcript = (empty_project.root / "corpus" / "alice-video.txt").read_text(encoding="utf-8")
        assert "[00:00.00 - 00:04.00] SPEAKER_00: Opening remarks." in transcript
        assert "[00:11.00 - 00:14.00] SPEAKER_01: Later response." in transcript

        video_stdout = empty_project.cli_ok("show", "video", "corpus/alice-video.txt")
        assert "alice.mp4" in video_stdout
        assert "chunk_count\t2" in video_stdout
        assert "00:08.00" in video_stdout
        assert "Later response." in video_stdout

        document_stdout = empty_project.cli_ok("show", "document", "corpus/alice-video.txt")
        assert "video_source" in document_stdout
        assert "alice.mp4" in document_stdout

    def test_show_video_fails_for_plain_text_document(self, project: BewleyProject) -> None:
        code, _, stderr = project.cli("show", "video", "corpus/interview_alice.txt")
        assert code != 0
        assert "no linked video source" in stderr
